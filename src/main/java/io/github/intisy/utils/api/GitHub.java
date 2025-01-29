package io.github.intisy.utils.api;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.intisy.simple.logger.Log;
import io.github.intisy.utils.custom.HttpDeleteWithBody;
import io.github.intisy.utils.custom.VersionAsset;
import io.github.intisy.utils.utils.EncryptorUtils;
import io.github.intisy.utils.utils.FileUtils;
import io.github.intisy.utils.utils.ThreadUtils;
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

@SuppressWarnings("unused")
public class GitHub {

    private final String API_URL = "https://api.github.com";
    private final String repoOwner;
    private final String repoName;
    private final String accessToken;
    private final boolean debug;
    private final List<File> deletedFiles = new ArrayList<>();
    private final List<File> createdFiles = new ArrayList<>();
    private final List<File> modifyFiles = new ArrayList<>();

    public GitHub(String repoOwner, String repoName) {
        this(repoOwner, repoName, null, false);
    }

    public GitHub(String repoOwner, String repoName, String accessToken) {
        this(repoOwner, repoName, accessToken, false);
    }

    public GitHub(String repoOwner, String repoName, String accessToken, boolean debug) {
        this.repoOwner = repoOwner;
        this.repoName = repoName;
        this.accessToken = accessToken;
        this.debug = debug;
    }

    public List<File> getCreatedFiles() {
        return createdFiles;
    }

    public List<File> getDeletedFiles() {
        return deletedFiles;
    }

    public List<File> getModifyFiles() {
        return modifyFiles;
    }

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

    public JsonObject deleteFolder(String folderPath) throws Exception {
        return deleteFolder(folderPath, true);
    }
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

    public void deleteFolder(File path) throws Exception {
        deleteFolder(path, "", true);
    }
    public void deleteFolder(File path, boolean encode) throws Exception {
        deleteFolder(path, "", encode);
    }
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
    public String fixString(String string) {
        return string.replace(" ", "%20");
    }
    public JsonObject deleteFile(String filePath) throws Exception {
        return deleteFile(filePath, true);
    }
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

    private List<String> readFile(String filePath, Path storagePath) {
        File file = storagePath.resolve(filePath).toFile();
        if (!file.exists()) {
            return new ArrayList<>();
        }
        Charset charset = StandardCharsets.ISO_8859_1;
        return FileUtils.readAllLines(file.toPath(), charset);
    }

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
                List<GHAsset> assets = targetRelease.getAssets();
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
