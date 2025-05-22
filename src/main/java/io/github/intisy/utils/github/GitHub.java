package io.github.intisy.utils.github;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.intisy.simple.logger.Log;
import io.github.intisy.utils.net.HttpDeleteWithBody;
import io.github.intisy.utils.security.EncryptorUtils;
import io.github.intisy.utils.core.FileUtils;
import io.github.intisy.utils.concurrency.ThreadUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.kohsuke.github.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * A utility class for interacting with the GitHub API.
 * This class provides methods for repository management, file operations,
 * and commit handling through the GitHub REST API.
 * It supports operations such as creating repositories, managing files,
 * downloading commits, and applying changes from commits.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class GitHub {

    /**
     * The base URL for the GitHub API.
     */
    private final String API_URL = "https://api.github.com";

    /**
     * The owner (user or organization) of the repository.
     */
    private final String repoOwner;

    /**
     * The name of the repository.
     */
    private final String repoName;

    /**
     * The access token for GitHub API authentication.
     */
    private final String accessToken;

    /**
     * Flag to enable debug logging.
     */
    private final boolean debug;

    /**
     * List of files that have been deleted during operations.
     */
    private final List<File> deletedFiles = new ArrayList<>();

    /**
     * List of files that have been created during operations.
     */
    private final List<File> createdFiles = new ArrayList<>();

    /**
     * List of files that have been modified during operations.
     */
    private final List<File> modifyFiles = new ArrayList<>();

    /**
     * Constructs a GitHub instance with the specified repository owner and name.
     * This constructor uses no access token and disables debug logging.
     *
     * @param repoOwner the owner (user or organization) of the repository
     * @param repoName the name of the repository
     */
    public GitHub(String repoOwner, String repoName) {
        this(repoOwner, repoName, null, false);
    }

    /**
     * Constructs a GitHub instance with the specified repository owner, name, and access token.
     * This constructor disables debug logging.
     *
     * @param repoOwner the owner (user or organization) of the repository
     * @param repoName the name of the repository
     * @param accessToken the access token for GitHub API authentication
     */
    public GitHub(String repoOwner, String repoName, String accessToken) {
        this(repoOwner, repoName, accessToken, false);
    }

    /**
     * Constructs a GitHub instance with the specified repository owner, name, access token, and debug flag.
     *
     * @param repoOwner the owner (user or organization) of the repository
     * @param repoName the name of the repository
     * @param accessToken the access token for GitHub API authentication
     * @param debug whether to enable debug logging
     */
    public GitHub(String repoOwner, String repoName, String accessToken, boolean debug) {
        this.repoOwner = repoOwner;
        this.repoName = repoName;
        this.accessToken = accessToken;
        this.debug = debug;
    }

    /**
     * Returns the list of files that have been created during operations.
     *
     * @return the list of created files
     */
    public List<File> getCreatedFiles() {
        return createdFiles;
    }

    /**
     * Returns the list of files that have been deleted during operations.
     *
     * @return the list of deleted files
     */
    public List<File> getDeletedFiles() {
        return deletedFiles;
    }

    /**
     * Returns the list of files that have been modified during operations.
     *
     * @return the list of modified files
     */
    public List<File> getModifyFiles() {
        return modifyFiles;
    }

    /**
     * Checks if the repository exists on GitHub.
     * This method sends a GET request to the GitHub API to check if the repository
     * specified by repoOwner and repoName exists.
     *
     * @return true if the repository exists, false otherwise
     * @throws IOException if an I/O error occurs during the API request
     */
    public boolean doesRepoExist() throws IOException {
        String url = String.format("%s/repos/%s/%s", API_URL, repoOwner, repoName);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3+json");
            Boolean value = null;
            String ss = null;
            while (value == null) {
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    JsonParser.parseString(responseString).getAsJsonObject();
                    if (statusCode == 200) {
                        value = true;
                    } else if (statusCode == 404) {
                        value = false;
                    }
                } catch (NullPointerException exception) {
                    Log.error("Wrong response: " + ss);
                    value = null;
                }
            }
            return value;
        }
    }
    /**
     * Downloads a specific commit from the GitHub repository as a ZIP archive and extracts it.
     * This method downloads the repository at the specified commit SHA, extracts the contents,
     * and places them in the specified output directory.
     *
     * @param sha the SHA-1 hash of the commit to download
     * @param outputDir the directory where the extracted repository should be placed
     */
    public void downloadGitHubCommit(String sha, Path outputDir) {
        try {
            String repoUrl = String.format("https://github.com/%s/%s/archive/%s.zip", repoOwner, repoName, sha);
            Path zipFilePath = downloadRepoAsZip(repoUrl, outputDir);
            unzip(zipFilePath, outputDir);
            if (debug)
                Log.debug("Repository downloaded successfully from commit SHA " + sha + "!");
        } catch (Exception e) {
            Log.error("Download failed: " + e.getMessage());
        }
    }

    /**
     * Downloads a repository as a ZIP file from the specified URL.
     * This method establishes an HTTP connection to the repository URL, downloads the ZIP file,
     * and saves it to a file in the parent directory of the specified output directory.
     *
     * @param repoUrl the URL of the repository ZIP file to download
     * @param outputDir the directory related to where the ZIP file should be saved
     * @return the path to the downloaded ZIP file
     * @throws IOException if an I/O error occurs during the download
     */
    private Path downloadRepoAsZip(String repoUrl, Path outputDir) throws IOException {
        URL url = new URL(repoUrl);
        String fileName = "repo.zip";
        Path zipFilePath = outputDir.getParent().resolve(fileName);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        if (accessToken != null)
            connection.setRequestProperty("Authorization", "token " + accessToken);
        try (InputStream in = connection.getInputStream();
             FileOutputStream fos = new FileOutputStream(zipFilePath.toFile())) {
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        } finally {
            connection.disconnect();
        }
        return zipFilePath;
    }

    /**
     * Extracts the contents of a ZIP file to a destination directory.
     * This method reads the ZIP file entry by entry, creating directories and files
     * as needed in the destination directory. It uses the newFile method to ensure
     * that extracted files are placed in the correct location.
     *
     * @param zipFilePath the path to the ZIP file to extract
     * @param destDirectory the directory where the ZIP contents should be extracted
     * @throws Exception if an error occurs during extraction
     */
    private void unzip(Path zipFilePath, Path destDirectory) throws Exception {
        File destDir = destDirectory.toFile();
        FileUtils.mkdirs(destDir);
        try (ZipInputStream zis = new ZipInputStream(Files.newInputStream(zipFilePath.toFile().toPath()))) {
            ZipEntry entry = zis.getNextEntry();
            assert entry != null;
            String name = entry.getName();
            while ((entry = zis.getNextEntry()) != null) {
                File newFile = newFile(destDir, entry, name);
                if (entry.isDirectory()) {
                    if (!newFile.isDirectory() && !newFile.mkdirs()) {
                        throw new IOException("Failed to create directory " + newFile);
                    }
                } else {
                    File parent = newFile.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs()) {
                        throw new IOException("Failed to create directory " + parent);
                    }
                    try (FileOutputStream fos = new FileOutputStream(newFile)) {
                        byte[] buffer = new byte[4096];
                        int length;
                        while ((length = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, length);
                        }
                    }
                }
            }
        }
    }
    /**
     * Creates a new file object for a ZIP entry, ensuring it's within the target directory.
     * This method decodes the ZIP entry name, creates a File object for it, and performs
     * a security check to ensure the file is within the target directory (preventing
     * directory traversal attacks).
     *
     * @param destinationDir the destination directory where files should be extracted
     * @param zipEntry the ZIP entry for which to create a file
     * @param name the base name to remove from the ZIP entry path
     * @return a File object representing the location where the ZIP entry should be extracted
     * @throws Exception if an error occurs during file creation or security validation
     */
    private File newFile(File destinationDir, ZipEntry zipEntry, String name) throws Exception {
        String decrypted = EncryptorUtils.decode(zipEntry.getName().replace(name, ""), "/");
        File destFile = new File(destinationDir, decrypted);
        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();
        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }
        return destFile;
    }

    /**
     * Deletes a folder from the GitHub repository.
     * This is a convenience method that calls deleteFolder(folderPath, true).
     *
     * @param folderPath the path of the folder to delete
     * @return a JsonObject containing the response from the GitHub API for the last deleted file
     * @throws Exception if an error occurs during the deletion process
     */
    public JsonObject deleteFolder(String folderPath) throws Exception {
        return deleteFolder(folderPath, true);
    }

    /**
     * Deletes a folder and all its contents from the GitHub repository.
     * This method retrieves all files in the specified folder and deletes them one by one.
     * The folder path can be optionally encoded for security.
     *
     * @param folderPath the path of the folder to delete
     * @param encode whether to encode the folder path
     * @return a JsonObject containing the response from the GitHub API for the last deleted file
     * @throws Exception if an error occurs during the deletion process
     */
    public JsonObject deleteFolder(String folderPath, boolean encode) throws Exception {
        JsonObject response = null;
        if (debug)
            Log.debug("Deleting folder: " + folderPath.substring(1));
        String encryptedFilePath;
        if (encode)
            encryptedFilePath = EncryptorUtils.encode(folderPath, "/");
        else
            encryptedFilePath = folderPath;
        JsonArray files = getFilesInFolder(encryptedFilePath);
        for (int i = 0; i < files.size(); i++) {
            JsonObject fileObject = files.get(i).getAsJsonObject();
            String filePath = fileObject.get("path").getAsString();
            response = deleteFile("/" + filePath, false);
        }
        return response;
    }

    /**
     * Deletes a folder from the local file system and the GitHub repository.
     * This is a convenience method that calls deleteFolder(path, "", true).
     *
     * @param path the folder to delete
     * @throws Exception if an error occurs during the deletion process
     */
    public void deleteFolder(File path) throws Exception {
        deleteFolder(path, "", true);
    }

    /**
     * Deletes a folder from the local file system and the GitHub repository.
     * This is a convenience method that calls deleteFolder(path, "", encode).
     *
     * @param path the folder to delete
     * @param encode whether to encode the folder path when deleting from GitHub
     * @throws Exception if an error occurs during the deletion process
     */
    public void deleteFolder(File path, boolean encode) throws Exception {
        deleteFolder(path, "", encode);
    }

    /**
     * Recursively deletes a folder and its contents from both the local file system and the GitHub repository.
     * This method traverses the directory structure, deleting files and subdirectories.
     * Files are deleted both locally and from the GitHub repository, except for .git files
     * which are only deleted locally.
     *
     * @param path the folder to delete
     * @param folder the relative path of the folder in the repository
     * @param encode whether to encode the folder path when deleting from GitHub
     * @throws Exception if an error occurs during the deletion process
     */
    public void deleteFolder(File path, String folder, boolean encode) throws Exception {
        if (!folder.isEmpty()) {
            if (debug)
                Log.debug("Deleting folder: " + folder.substring(1));
        }
        File[] files = path.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteFolder(file, folder + "/" + file.getName(), encode);
                } else {
                    FileUtils.delete(file);
                    if (!file.getAbsolutePath().contains(".git")) {
                        deleteFile(folder + "/" + file.getName(), encode);
                    }
                }
            }
        }
        FileUtils.delete(path);
    }

    /**
     * Retrieves a list of files in a folder from the GitHub repository.
     * This method sends a GET request to the GitHub API to retrieve the contents
     * of the specified folder. The response is parsed into a JsonArray containing
     * information about each file or directory in the folder.
     *
     * @param folderPath the path of the folder whose contents should be retrieved
     * @return a JsonArray containing information about the files and directories in the folder
     * @throws IOException if an I/O error occurs during the API request
     */
    private JsonArray getFilesInFolder(String folderPath) throws IOException {
        String url = String.format("%s/repos/%s/%s/contents/%s", API_URL, repoOwner, repoName, fixString(folderPath));

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3+json");

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                String responseString = EntityUtils.toString(entity);
                JsonElement jsonElement = JsonParser.parseString(responseString);
                if (jsonElement.isJsonArray())
                    return jsonElement.getAsJsonArray();
                else {
                    JsonArray jsonArray = new JsonArray();
                    jsonArray.add(jsonElement);
                    return jsonArray;
                }
            }
        }
    }
    /**
     * Creates a new repository on GitHub.
     * This method creates a new repository with the specified description and privacy setting.
     * If the repository already exists, an empty JsonObject is returned.
     *
     * @param description the description of the repository
     * @param isPrivate whether the repository should be private (true) or public (false)
     * @return a JsonObject containing the GitHub API response, or an empty JsonObject if the repository already exists
     * @throws IOException if an I/O error occurs during the API request
     */
    public JsonObject createRepo(String description, boolean isPrivate) throws IOException {
        if (!doesRepoExist()) {
            String url = API_URL + "/user/repos";

            JsonObject json = new JsonObject();
            json.addProperty("name", repoName);
            json.addProperty("description", description);
            json.addProperty("private", isPrivate);
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost httpPost = new HttpPost(url);
                if (accessToken != null)
                    httpPost.setHeader("Authorization", "token " + accessToken);
                httpPost.setHeader("Accept", "application/vnd.github.v3+json");
                httpPost.setEntity(new StringEntity(json.toString()));
                JsonObject jsonObject = null;
                while (jsonObject == null) {
                    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                        HttpEntity entity = response.getEntity();
                        String responseString = EntityUtils.toString(entity);
                        jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                    } catch (NullPointerException exception) {
                        Log.error("Wrong response: " + jsonObject);
                        jsonObject = null;
                    }
                }
                return jsonObject;
            }
        } else
            return new JsonObject();
    }

    /**
     * Creates a new file in the GitHub repository.
     * This method creates a new file with the specified content at the given path.
     * The file path is encoded for security, and the content is Base64 encoded.
     *
     * @param filePath the path where the file should be created (should start with a slash)
     * @param fileContent the content of the file to create
     * @return a JsonObject containing the GitHub API response
     * @throws Exception if an error occurs during the file creation process
     */
    public JsonObject createFile(String filePath, String fileContent) throws Exception {
        filePath = filePath.substring(1);
        String encryptedFilePath = EncryptorUtils.encode(filePath, "/");
        if (debug)
            Log.debug("Creating file: " + filePath + ", as " + encryptedFilePath);
        String commitMessage = "Create " + filePath;
        String encodedContent = Base64.getEncoder().encodeToString(fileContent.getBytes());
        String url = String.format("%s/repos/%s/%s/contents/%s", API_URL, repoOwner, repoName, fixString(encryptedFilePath));
        JsonObject json = new JsonObject();
        json.addProperty("message", commitMessage);
        json.addProperty("content", encodedContent);
        JsonObject jsonObject = null;
        while (jsonObject == null) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPut httpPut = new HttpPut(url);
                if (accessToken != null)
                    httpPut.setHeader("Authorization", "token " + accessToken);
                httpPut.setHeader("Accept", "application/vnd.github.v3+json");
                httpPut.setEntity(new StringEntity(json.toString()));
                try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                }
            } catch (NullPointerException exception) {
                Log.error("Wrong response: " + jsonObject);
                jsonObject = null;
            }
        }
        return jsonObject;
    }
    /**
     * Updates an existing file in the GitHub repository.
     * This method updates the content of an existing file at the given path.
     * The file path is encoded for security, and the content is Base64 encoded.
     * The SHA of the current file is required to update it.
     *
     * @param filePath the path of the file to update (should start with a slash)
     * @param fileContent the new content of the file
     * @return a JsonObject containing the GitHub API response
     * @throws Exception if an error occurs during the file update process
     */
    public JsonObject updateFile(String filePath, String fileContent) throws Exception {
        filePath = filePath.substring(1);
        if (debug)
            Log.debug("Updating file: " + filePath);
        String commitMessage = "Update " + filePath;
        filePath = EncryptorUtils.encode(filePath, "/");
        String encodedContent = Base64.getEncoder().encodeToString(fileContent.getBytes());
        String url = String.format("%s/repos/%s/%s/contents/%s", API_URL, repoOwner, repoName, fixString(filePath));
        String sha = getFileSha(filePath);
        JsonObject json = new JsonObject();
        json.addProperty("message", commitMessage);
        json.addProperty("content", encodedContent);
        if (sha != null) {
            json.addProperty("sha", sha);
        }
        JsonObject jsonObject = null;
        while (jsonObject == null) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPut httpPut = new HttpPut(url);
                if (accessToken != null)
                    httpPut.setHeader("Authorization", "token " + accessToken);
                httpPut.setHeader("Accept", "application/vnd.github.v3+json");
                httpPut.setEntity(new StringEntity(json.toString()));
                try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                }
            } catch (NullPointerException exception) {
                Log.error("Wrong response: " + jsonObject);
                jsonObject = null;
            }
        }
        return jsonObject;
    }
    /**
     * Replaces spaces in a string with URL-encoded spaces (%20).
     * This method is used to prepare strings for inclusion in URLs.
     *
     * @param string the string to fix
     * @return the string with spaces replaced by %20
     */
    public String fixString(String string) {
        return string.replace(" ", "%20");
    }

    /**
     * Deletes a file from the GitHub repository.
     * This is a convenience method that calls deleteFile(filePath, true).
     *
     * @param filePath the path of the file to delete
     * @return a JsonObject containing the GitHub API response
     * @throws Exception if an error occurs during the file deletion process
     */
    public JsonObject deleteFile(String filePath) throws Exception {
        return deleteFile(filePath, true);
    }

    /**
     * Deletes a file from the GitHub repository.
     * This method deletes a file at the given path. The file path can be optionally encoded
     * for security. The SHA of the current file is required to delete it.
     *
     * @param filePath the path of the file to delete (should start with a slash)
     * @param encode whether to encode the file path
     * @return a JsonObject containing the GitHub API response
     * @throws Exception if an error occurs during the file deletion process
     */
    public JsonObject deleteFile(String filePath, boolean encode) throws Exception {
        filePath = filePath.substring(1);
        String encryptedFilePath;
        if (encode)
            encryptedFilePath = EncryptorUtils.encode(filePath, "/");
        else
            encryptedFilePath = filePath;
        if (debug)
            Log.debug("Deleting file: " + filePath + ", as " + encryptedFilePath);
        String commitMessage = "Delete " + filePath;
        String sha = getFileSha(encryptedFilePath);
        String url = String.format("%s/repos/%s/%s/contents/%s", API_URL, repoOwner, repoName, fixString(encryptedFilePath));
        JsonObject jsonObject = null;
        while (jsonObject == null) {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpDeleteWithBody httpDelete = getHttpDeleteWithBody(commitMessage, url, sha);
                try (CloseableHttpResponse response = httpClient.execute(httpDelete)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                }
            } catch (NullPointerException exception) {
                Log.error("Wrong response: " + jsonObject);
                jsonObject = null;
            }
        }
        return jsonObject;
    }

    /**
     * Creates an HTTP DELETE request with a body for deleting a file from GitHub.
     * This method creates a custom HttpDeleteWithBody object with the necessary headers
     * and a JSON payload containing the commit message, SHA, and branch.
     *
     * @param commitMessage the commit message for the deletion
     * @param url the URL of the file to delete
     * @param sha the SHA of the file to delete
     * @return an HttpDeleteWithBody object configured for the deletion request
     */
    private HttpDeleteWithBody getHttpDeleteWithBody(String commitMessage, String url, String sha) {
        HttpDeleteWithBody httpDelete = new HttpDeleteWithBody(url);
        if (accessToken != null)
            httpDelete.setHeader("Authorization", "token " + accessToken);
        httpDelete.setHeader("Accept", "application/vnd.github.v3+json");

        // Creating the JSON payload for the commit
        JsonObject json = new JsonObject();
        json.addProperty("message", commitMessage);
        json.addProperty("sha", sha);
        json.addProperty("branch", "main");

        StringEntity entity = new StringEntity(json.toString(), StandardCharsets.UTF_8);
        httpDelete.setEntity(entity);
        return httpDelete;
    }

    /**
     * Retrieves the SHA-1 hash of a file in the GitHub repository.
     * This method sends a GET request to the GitHub API to retrieve information about
     * the specified file, including its SHA-1 hash which is required for update and delete operations.
     *
     * @param filePath the path of the file whose SHA should be retrieved
     * @return the SHA-1 hash of the file, or null if the file does not exist or an error occurs
     */
    private String getFileSha(String filePath) {
        String url = String.format("%s/repos/%s/%s/contents/%s", API_URL, repoOwner, repoName, fixString(filePath));
        JsonObject jsonObject = null;
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3+json");
            while (jsonObject == null) {
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    JsonElement responseJson = JsonParser.parseString(responseString);
                    if (responseJson.isJsonArray())
                        jsonObject = responseJson.getAsJsonArray().get(0).getAsJsonObject();
                    else
                        jsonObject = responseJson.getAsJsonObject();
                } catch (NullPointerException exception) {
                    Log.error("Wrong response: " + jsonObject);
                    jsonObject = null;
                }
            }
            return jsonObject.get("sha").getAsString();
        } catch (Exception e) {
//            Log.error("Error getting file SHA: " + e.getMessage());
//            Log.printStackTrace(null);
            return null;
        }
    }
    /**
     * Applies a series of commits to a local storage path.
     * This method processes a list of commits in reverse order (oldest first) and applies
     * the changes from each commit to the specified storage path. The changes are applied
     * by fetching the diff for each commit and processing it.
     *
     * @param commits a JsonArray containing the commits to apply
     * @param storagePath the local path where the changes should be applied
     * @throws Exception if an error occurs during the application of changes
     */
    public void applyChanges(JsonArray commits, Path storagePath) throws Exception {
        List<JsonElement> commitElements = new ArrayList<>();
        for (int i = commits.size()-1; i >= 0; i--) {
            commitElements.add(commits.get(i));
        }
        for (JsonElement commitElement : commitElements) {
            JsonObject commitObject = commitElement.getAsJsonObject();
            String sha = commitObject.get("sha").getAsString();
            String message = commitObject.get("commit").getAsJsonObject().get("message").getAsString();
            Log.debug("Applying commit " + sha + ": " + message);
            processDiff(fetchCommitDiff(sha), storagePath);
        }
    }

    /**
     * Processes a Git diff and applies the changes to the local storage path.
     * This method parses a Git diff, extracts the file paths and content changes,
     * and writes the updated files to the specified storage path. It handles new files,
     * deleted files, and modifications to existing files.
     *
     * @param diff the Git diff to process
     * @param storagePath the local path where the changes should be applied
     * @throws Exception if an error occurs during the processing of the diff
     */
    private void processDiff(String diff, Path storagePath) throws Exception {
        String[] lines = diff.split("\n");
        String currentFile = null;
        List<String> fileContent = new ArrayList<>();
        boolean isNewFile = false;
        boolean isDeletedFile = false;
        boolean inHunk = false;
        int count = 0;
        Log.debug(" -- from diff: -- ");
        Log.debug(diff);
        for (String line : lines) {
            if (line.startsWith("diff --git")) {
                if (currentFile != null) {
                    isDeletedFile = false;
                    writeFile(currentFile, storagePath, fileContent, isNewFile);
                }
                String[] parts = line.substring(13).split(" b/");
                if (parts.length == 2) {
                    currentFile = parts[0];
                } else {
                    currentFile = line.substring(13).split(parts[parts.length - 1])[0];
                }
                currentFile = EncryptorUtils.decode(currentFile, "/");
                fileContent = new ArrayList<>();
                isNewFile = false;
                inHunk = false;
            } else if (!isDeletedFile) {
                if (line.startsWith("new file mode")) {
                    isNewFile = true;
                } else if (line.startsWith("deleted file mode")) {
                    assert currentFile != null;
                    File file = storagePath.resolve(currentFile).toFile();
                    deletedFiles.add(file);
                    if (!file.delete())
                        throw new IOException("Failed to delete file: " + currentFile);
                    currentFile = null;
                    File parent = file.getParentFile();
                    if (!parent.equals(storagePath.toFile()) || parent.listFiles() == null || Objects.requireNonNull(parent.listFiles()).length == 0) {
                        if (!parent.delete())
                            throw new IOException("Failed to delete directory: " + parent);
                    }
                    isDeletedFile = true;
                } else if (line.startsWith("---")) {
                    if (!isNewFile && !line.contains("/dev/null")) {
                        fileContent = readFile(currentFile, storagePath);
                    }
                } else if (line.startsWith("@@")) {
                    inHunk = true;
                    count = Integer.parseInt(line.split("\\+")[1].split(",")[0].split(" @@")[0]);
                    if (count == 0) {
                        count = Integer.parseInt(line.split("-")[1].split(",")[0].split(" ")[0]);
                    }
                } else if (inHunk) {
                    if (line.startsWith("+")) {
                        if (line.length() == 1)
                            fileContent.add("");
                        else
                            fileContent.add(count-1, line.substring(1, line.length()-1));
                        count++;
                    } else if (line.startsWith("-")) {
                        fileContent.remove(count-1);
                    } else {
                        count++;
                    }
                }
            }
        }
        if (currentFile != null) {
            writeFile(currentFile, storagePath, fileContent, isNewFile);
        }
    }

    /**
     * Writes content to a file in the local storage path.
     * This method creates or updates a file with the specified content.
     * If the file is new, it creates the necessary parent directories.
     * The file is added to the createdFiles and modifyFiles lists for tracking.
     *
     * @param filePath the path of the file to write
     * @param storagePath the base storage path
     * @param content the content to write to the file
     * @param isNewFile whether the file is new (true) or existing (false)
     * @throws Exception if an error occurs during the file writing process
     */
    private void writeFile(String filePath, Path storagePath, List<String> content, boolean isNewFile) throws Exception {
        File file = storagePath.resolve(filePath).toFile();
        createdFiles.add(file);
        if (isNewFile) {
            FileUtils.mkdirs(file.getParentFile());
            FileUtils.createNewFile(file);
        }

        try (FileWriter writer = new FileWriter(file)) {
            modifyFiles.add(file);
            for (String line : content) {
                writer.write(line + System.lineSeparator());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads the content of a file from the local storage path.
     * This method reads the content of the specified file and returns it as a list of strings.
     * If the file does not exist, an empty list is returned.
     *
     * @param filePath the path of the file to read
     * @param storagePath the base storage path
     * @return a list of strings containing the lines of the file
     */
    private List<String> readFile(String filePath, Path storagePath) {
        File file = storagePath.resolve(filePath).toFile();
        if (!file.exists()) {
            return new ArrayList<>();
        }
        Charset charset = StandardCharsets.ISO_8859_1;
        return FileUtils.readAllLines(file.toPath(), charset);
    }

    /**
     * Retrieves all commits since a specified commit.
     * This method retrieves all commits that have been made since the specified commit SHA.
     * The commits are returned in reverse chronological order (newest first).
     *
     * @param commitSha the SHA-1 hash of the commit from which to start retrieving commits
     * @return a JsonArray containing the commits since the specified commit
     * @throws IOException if an I/O error occurs during the API request
     */
    public JsonArray getCommitsSince(String commitSha) throws IOException {
        JsonArray commitsArray = new JsonArray();
        JsonObject commit = getLastCommit().getAsJsonArray().get(0).getAsJsonObject();
        if (!commit.get("sha").getAsString().equals(commitSha)) {
            String parent;
            while (!(parent = commit.get("parents").getAsJsonArray().get(0).getAsJsonObject().get("sha").getAsString()).equals(commitSha)) {
                commitsArray.add(commit);
                commit = getCommit(parent);
            }
            commitsArray.add(commit);
            return commitsArray;
        } else
            return new JsonArray();
    }
    /**
     * Retrieves information about a specific commit.
     * This method sends a GET request to the GitHub API to retrieve detailed information
     * about the specified commit.
     *
     * @param commitSha the SHA-1 hash of the commit to retrieve
     * @return a JsonObject containing information about the commit
     * @throws IOException if an I/O error occurs during the API request
     */
    public JsonObject getCommit(String commitSha) throws IOException {
        String url = String.format("%s/repos/%s/%s/commits/%s", API_URL, repoOwner, repoName, commitSha);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3+json");
            JsonObject jsonObject = null;
            while (jsonObject == null) {
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
                } catch (NullPointerException exception) {
                    Log.error("Wrong response: " + jsonObject);
                    jsonObject = null;
                }
            }
            return jsonObject;
        }
    }

    /**
     * Retrieves the most recent commit in the repository.
     * This method sends a GET request to the GitHub API to retrieve information
     * about the most recent commit in the repository.
     *
     * @return a JsonElement containing information about the most recent commit
     * @throws IOException if an I/O error occurs during the API request
     */
    public JsonElement getLastCommit() throws IOException {
        String url = String.format("%s/repos/%s/%s/commits?per_page=1", API_URL, repoOwner, repoName);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3+json");
            JsonElement jsonObject = null;
            while (jsonObject == null) {
                try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                    HttpEntity entity = response.getEntity();
                    String responseString = EntityUtils.toString(entity);
                    jsonObject = JsonParser.parseString(responseString);
                } catch (NullPointerException exception) {
                    Log.error("Wrong response: " + jsonObject);
                    jsonObject = null;
                }
            }
            return jsonObject;

        }
    }

    /**
     * Fetches the diff for a specific commit.
     * This method sends a GET request to the GitHub API to retrieve the diff
     * for the specified commit. The diff shows the changes made in the commit.
     *
     * @param commitSha the SHA-1 hash of the commit whose diff should be fetched
     * @return a string containing the diff for the commit
     * @throws IOException if an I/O error occurs during the API request
     */
    public String fetchCommitDiff(String commitSha) throws IOException {
        String url = String.format("%s/repos/%s/%s/commits/%s", API_URL, repoOwner, repoName, commitSha);

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            if (accessToken != null)
                httpGet.setHeader("Authorization", "token " + accessToken);
            httpGet.setHeader("Accept", "application/vnd.github.v3.diff");

            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                HttpEntity entity = response.getEntity();
                return EntityUtils.toString(entity);
            }
        }
    }
    /**
     * Retrieves an asset from the latest release of the repository.
     * This method searches for the latest release of the repository and retrieves
     * the specified asset from that release. The latest release is determined by
     * comparing version numbers in the release tags.
     *
     * @param fileName the name of the asset to retrieve
     * @return a VersionAsset object containing the asset and its version
     * @throws RuntimeException if the asset cannot be found
     */
    public VersionAsset getAsset(String fileName) {
        Log.debug("Searching for newest jar file from " + repoName + " assets...");
        try {
            org.kohsuke.github.GitHub github = accessToken != null ? org.kohsuke.github.GitHub.connectUsingOAuth(accessToken) : org.kohsuke.github.GitHub.connect();
            List<GHRelease> releases = github.getRepository(repoOwner + "/" + repoName).listReleases().toList();

            GHRelease targetRelease = null;
            double top = 0;
            for (GHRelease release : releases) {
                String tag = release.getTagName();
                int divider = 1;
                double current = 0;
                for (String number : tag.split("\\.")) {
                    current += Double.parseDouble(number) / divider;
                    divider *= 1000;
                }
                if (current > top) {
                    top = current;
                    targetRelease = release;
                }
            }

            if (targetRelease != null) {
                List<GHAsset> assets = targetRelease.listAssets().toList();
                if (!assets.isEmpty()) {
                    Log.debug("Found " + assets.size() + " asset(s) in the release");
                    for (GHAsset asset : assets) {
                        if (asset.getName().equals(fileName))
                            return new VersionAsset(targetRelease.getTagName(), asset);
                    }
                } else {
                    Log.warning("No assets found for the release");
                }
            } else{
                Log.warning("Release not found");
            }
        } catch (IOException e) {
            Log.warning("Github exception while pulling asset: " + e.getMessage() + " (retrying in 5 seconds...)");
            ThreadUtils.sleep(5000);
            return getAsset(fileName);
        }
        throw new RuntimeException("Could not find an valid asset");
    }
    public String getLatestTag() {
        Log.debug("Searching for newest jar file from " + repoName + " assets...");
        try {
            org.kohsuke.github.GitHub github = accessToken != null ? org.kohsuke.github.GitHub.connectUsingOAuth(accessToken) : org.kohsuke.github.GitHub.connect();
            GHRepository repo = github.getRepository(repoOwner + "/" + repoName);
            PagedIterable<GHTag> tags = repo.listTags();
            GHTag latestTag = tags.iterator().next();
            return latestTag.getName();
        } catch (IOException e) {
            Log.warning("Github exception while pulling asset: " + e.getMessage());
        }
        throw new RuntimeException("Could not find an valid asset");
    }

    public String getLastSha() throws IOException {
        return getLastCommit().getAsJsonArray().get(0).getAsJsonObject().get("sha").getAsString();
    }

    public void jar(File direction, GHAsset asset, String repoName, String repoOwner) throws IOException {
        String assetName = asset.getName();
        String downloadUrl = "https://api.github.com/repos/" + repoOwner + "/" + repoName + "/releases/assets/" + asset.getId();
        Log.note("Downloading jar file from Github assets... (" + downloadUrl + ")");

        HttpURLConnection connection = (HttpURLConnection) new URL(downloadUrl).openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "application/octet-stream");
        if (accessToken != null)
            connection.setRequestProperty("Authorization", "Bearer " + accessToken);

        int responseCode = connection.getResponseCode();

        if (responseCode != HttpURLConnection.HTTP_OK) {
            Log.warning("Failed to download asset: " + responseCode + " (retrying in 5 seconds...)");
            ThreadUtils.sleep(5000);
            connection.disconnect();
            jar(direction, asset, repoName, repoOwner);
        } else {
            // Read the response body and write to the file
            try (InputStream inputStream = connection.getInputStream();
                 FileOutputStream fos = new FileOutputStream(direction)) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
                Log.success("Downloaded and saved asset: " + assetName);
            } catch (IOException e) {
                Log.error("Error writing file: " + e.getMessage());
                throw e;
            } finally {
                connection.disconnect();
            }
        }
    }
}
