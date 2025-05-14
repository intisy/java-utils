package io.github.intisy.utils.utils;

import io.github.intisy.simple.logger.Log;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings({"unused", "ResultOfMethodCallIgnored"})
public class FileUtils {
    public static List<String> readAllLines(Path directory, Charset charset) {
        while (true) {
            try {
                return Files.readAllLines(directory, charset);
            } catch (IOException e) {
                Log.warning("Exception while reading file, waiting for problem to resolve...");
            }
        }
    }
    public static void delete(File file) {
        if (file.exists())
            if (!file.delete())
                throw new RuntimeException("Failed to delete file " + file);
    }
    public static void mkdirs(String file) {
        mkdirs(new File(file));
    }
    public static void mkdirs(File file) {
        if (!file.exists())
            if (!file.mkdirs())
                throw new RuntimeException("Failed to make folder " + file);
    }
    public static void createNewFile(File file) throws IOException {
        if (!file.exists())
            if (!file.createNewFile())
                throw new RuntimeException("Failed to create file " + file);
    }
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

                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
    public static File cleanFile(String file) {
        File f = new File(file);
        if (f.exists())
            if (!f.delete())
                throw new RuntimeException("Could not delete old file: " + file);
        return f;
    }
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

    @SuppressWarnings("resource")
    public List<String> listResourceFiles(String folder) throws URISyntaxException {
        URL dirURL = getClass().getClassLoader().getResource(folder);
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
