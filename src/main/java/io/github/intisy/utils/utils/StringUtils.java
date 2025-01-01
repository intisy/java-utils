package io.github.intisy.utils.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SuppressWarnings("unused")
public class StringUtils {
    public static String censor(String key) {
        String censored = key;
        if (key.length() > 3) {
            censored = key.substring(0, 3) + "...";
            if (key.length() > 7) {
                censored = censored + key.substring(key.length()-3);
            }
        }
        return censored;
    }

    public static String value(String value, String key) {
        int i = value.indexOf(key) + key.length();
        return value.substring(i, value.indexOf("\"\n", i));
    }

    public static String replaceSpecialCharacters(String input, String replacement) {
        Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
        Matcher matcher = pattern.matcher(input);
        return matcher.replaceAll(replacement);
    }

    public static String generateUniqueString(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    public static List<String[]> parseData(List<String> inputData) {
        List<String[]> result = new ArrayList<>();
        for (String line : inputData) {
            String[] parts = line.split(", ");
            result.add(parts);
        }
        return result;
    }

    public static String encrypt(String text, String key) {
        StringBuilder encrypted = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            int charValue = text.charAt(i);
            int keyValue = key.charAt(i % key.length());
            // XOR operation between character value and key value
            int encryptedValue = charValue ^ keyValue;
            encrypted.append((char) encryptedValue);
        }
        return encrypted.toString();
    }

    public static String decrypt(String encryptedText, String key) {
        return encrypt(encryptedText, key); // XOR encryption is its own decryption
    }

    public static String encrypt(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(password.getBytes());
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static List<String> splitString(String string) {
        List<String> result = new ArrayList<>();
        StringBuilder line = new StringBuilder();
        int i = 0;
        for (String l : (string+" .").split(" ")) {
            i++;
            if (line.toString().isEmpty()) {
                line.append(l);
            } else if (line.length()+l.length()+1 <= 22 && string.split(" ").length+1 != i) {
                line.append(" ").append(l);
            } else {
                result.add(line.toString());
                line = new StringBuilder();
                line.append(l);
            }
        }
        return result;
    }
}
