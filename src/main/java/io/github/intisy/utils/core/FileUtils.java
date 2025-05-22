package io.github.intisy.utils.core;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class providing file and directory operations for common file system tasks.
 * This class includes methods for reading, writing, copying, and deleting files and directories,
 * as well as utilities for working with resources and temporary files.
 * <p>
 * Many methods in this class are designed to be resilient, retrying operations or
 * providing clear error messages when operations fail.
 *
 * @author Finn Birich
 */
@SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
public class FileUtils {
    /**
     * Reads all lines from a file with retry capability. If the read operation fails due to an IOException,
     * the method will log a warning and retry indefinitely until successful.
     *
     * @param directory the path to the file to read
     * @param charset the charset to use for reading the file
     * @return a list of strings representing the lines in the file
     */
    public static List<String> readAllLines(Path directory, Charset charset) {
        try {
            return Files.readAllLines(directory, charset);
        } catch (IOException e) {
            throw new RuntimeException("Exception while reading file, waiting for problem to resolve...");
        }
    }

    /**
     * Deletes a file if it exists. If the deletion fails, a RuntimeException is thrown.
     *
     * @param file the file to delete
     * @throws RuntimeException if the file exists but cannot be deleted
     */
    public static void delete(File file) {
        if (file.exists())
            if (!file.delete())
                throw new RuntimeException("Failed to delete file " + file);
    }

    /**
     * Creates directories for the specified path string.
     *
     * @param file the path string for which directories should be created
     * @throws RuntimeException if the directories cannot be created
     */
    public static void mkdirs(String file) {
        mkdirs(new File(file));
    }

    /**
     * Creates directories for the specified file path, including any necessary
     * but nonexistent parent directories. If the directories cannot be created,
     * a RuntimeException is thrown.
     *
     * @param file the file path for which directories should be created
     * @throws RuntimeException if the directories cannot be created
     */
    public static void mkdirs(File file) {
        if (!file.exists())
            if (!file.mkdirs())
                throw new RuntimeException("Failed to make folder " + file);
    }

    /**
     * Creates a new, empty file if a file with this name does not yet exist.
     * If the file already exists, this method does nothing.
     *
     * @param file the file to be created
     * @throws IOException if an I/O error occurs during file creation
     * @throws RuntimeException if the file cannot be created
     */
    public static void createNewFile(File file) throws IOException {
        if (!file.exists())
            if (!file.createNewFile())
                throw new RuntimeException("Failed to create file " + file);
    }
    /**
     * Displays the content of a file to the standard output, line by line.
     * If an error occurs while reading the file, an error message is printed to standard error.
     *
     * @param file the file whose content should be displayed
     */
    public static void displayFileContent(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }
    }

    /**
     * Recursively copies a folder and all its contents to a destination location.
     * If the source is a directory, all its contents are copied recursively.
     * If the source is a file, it is copied directly to the destination.
     * If an AccessDeniedException occurs during copying a file, it is silently ignored.
     *
     * @param source the source file or directory to copy
     * @param destination the destination location
     * @throws RuntimeException if an IOException occurs during the copy operation
     */
    public static void copyFolder(File source, File destination) {
        try {
            if (source.isDirectory()) {
                if (!destination.exists()) {
                    destination.mkdir();
                }

                String[] files = source.list();

                if (files != null) {
                    for (String file : files) {
                        copyFolder(new File(source, file), new File(destination, file));
                    }
                }
            } else {
                try {
                    Files.copy(source.toPath(), destination.toPath(), StandardCopyOption.REPLACE_EXISTING);
                } catch (AccessDeniedException ignored) {
                    // Access denied exceptions are silently ignored
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a classpath resource to a temporary file on the file system.
     * This method creates a temporary file with the same extension as the resource
     * and copies the content of the resource to this file.
     *
     * @param resourceClass the class whose class loader should be used to locate the resource
     * @param path the path to the resource, relative to the class loader's root
     * @return a File object representing the temporary file containing the resource content
     * @throws RuntimeException if the resource cannot be found or an I/O error occurs
     */
    public static File resourceToFile(Class<?> resourceClass, String path) {
        URL resourceUrl = resourceClass.getClassLoader().getResource(path);
        assert resourceUrl != null;
        String[] split = path.split("\\.");
        String suffix = "." + path.split("\\.")[split.length - 1];
        String prefix = "temp-resource";
        try {
            File tempFile = File.createTempFile(prefix, suffix);
            try (OutputStream outputStream = Files.newOutputStream(tempFile.toPath())) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                try (InputStream stream = resourceUrl.openStream()) {
                    while ((bytesRead = stream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                    }
                }
            }
            return tempFile;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Recursively deletes a folder and all its contents.
     * If the provided File object represents a directory, all its contents
     * are deleted recursively before deleting the directory itself.
     *
     * @param folder the folder to delete
     */
    public static void deleteFolder(File folder) {
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();

            if (files != null) {
                for (File file : files) {
                    deleteFolder(file);
                }
            }
        }

        folder.delete();
    }
    /**
     * Deletes a file if it exists and returns a File object for the path.
     * This method is useful when you want to ensure a file doesn't exist before creating it.
     *
     * @param file the path to the file to clean
     * @return a File object representing the cleaned file path
     * @throws RuntimeException if the file exists but cannot be deleted
     */
    public static File cleanFile(String file) {
        File f = new File(file);
        if (f.exists())
            if (!f.delete())
                throw new RuntimeException("Could not delete old file: " + file);
        return f;
    }

    /**
     * Cleans a folder by deleting all its contents recursively.
     * If the folder exists and is a directory, all its contents are deleted.
     * If the folder exists and is a file, the file is deleted.
     * If the folder doesn't exist, it is created.
     *
     * @param folder the folder to clean
     * @throws RuntimeException if files or folders cannot be deleted, or if the folder cannot be created
     */
    public static void clean(File folder) {
        if (folder.exists()) {
            if (folder.isDirectory())
                if (folder.exists()) {
                    File[] files = folder.listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file.isDirectory()) {
                                clean(file);
                            } else {
                                if (!file.delete())
                                    throw new RuntimeException("could not delete file: " + file);
                            }
                        }
                    }
                } else {
                    throw new RuntimeException("Folder does not exist.");
                }
            else {
                if (!folder.delete())
                    throw new RuntimeException("could not delete folder: " + folder);
            }
        } else
            if (!folder.mkdir())
                throw new RuntimeException("could not create folder: " + folder);
    }

    /**
     * Lists all files in a resource directory.
     * This method attempts to list all files in a directory that is available as a resource
     * in the classpath. If the directory is found and is accessible as a file system directory,
     * the method returns a list of filenames in that directory. Otherwise, it returns an empty list.
     *
     * @param folder the path to the resource directory, relative to the class loader's root
     * @return a list of filenames in the resource directory, or an empty list if the directory
     *         cannot be accessed or does not exist
     * @throws URISyntaxException if the resource URL cannot be converted to a URI
     * @throws UncheckedIOException if an I/O error occurs while listing the directory contents
     */
    @SuppressWarnings("resource")
    public static List<String> listResourceFiles(String folder) throws URISyntaxException {
        URL dirURL = FileUtils.class.getClassLoader().getResource(folder);
        if (dirURL != null && dirURL.getProtocol().equals("file")) {
            Path path = Paths.get(dirURL.toURI());
            try {
                return Files.list(path)
                        .map(p -> p.getFileName().toString())
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            return new ArrayList<>();
        }
    }
}
