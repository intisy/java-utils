package io.github.intisy.utils.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class providing string manipulation and processing operations.
 * This class includes methods for string encryption, formatting, parsing, and transformation.
 * It provides functionality for censoring sensitive information, generating unique strings,
 * and performing various text operations.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class StringUtils {
    /**
     * Censors a string by showing only the first few and last few characters.
     * This method is useful for displaying sensitive information like API keys
     * in a way that doesn't reveal the full content.
     *
     * @param key the string to censor
     * @return a censored version of the input string
     */
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

    /**
     * Extracts a value from a string based on a key.
     * This method finds the key in the input string and extracts the text
     * between the end of the key and the next occurrence of "\"\n".
     *
     * @param value the string to extract from
     * @param key the key to search for
     * @return the extracted value
     * @throws StringIndexOutOfBoundsException if the key is not found or the format is incorrect
     */
    public static String value(String value, String key) {
        int i = value.indexOf(key) + key.length();
        return value.substring(i, value.indexOf("\"\n", i));
    }

    /**
     * Replaces all special characters in a string with a specified replacement.
     * Special characters are defined as any character that is not a letter (a-z, A-Z)
     * or a digit (0-9).
     *
     * @param input the string to process
     * @param replacement the string to replace special characters with
     * @return the processed string with special characters replaced
     */
    public static String replaceSpecialCharacters(String input, String replacement) {
        Pattern pattern = Pattern.compile("[^a-zA-Z0-9]");
        Matcher matcher = pattern.matcher(input);
        return matcher.replaceAll(replacement);
    }

    /**
     * Generates a unique string based on the input using SHA-256 hashing.
     * The result is encoded using URL-safe Base64 encoding without padding.
     *
     * @param input the string to generate a unique string from
     * @return a unique string based on the input
     * @throws RuntimeException if the SHA-256 algorithm is not available
     */
    public static String generateUniqueString(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    /**
     * Parses a list of strings into a list of string arrays by splitting each line by ", ".
     *
     * @param inputData the list of strings to parse
     * @return a list of string arrays, where each array contains the parts of a line
     */
    public static List<String[]> parseData(List<String> inputData) {
        List<String[]> result = new ArrayList<>();
        for (String line : inputData) {
            String[] parts = line.split(", ");
            result.add(parts);
        }
        return result;
    }

    /**
     * Encrypts a string using XOR encryption with the provided key.
     * Each character in the input string is XORed with a character from the key.
     * If the key is shorter than the input, it is repeated cyclically.
     *
     * @param text the string to encrypt
     * @param key the encryption key
     * @return the encrypted string
     */
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

    /**
     * Decrypts a string that was encrypted using XOR encryption.
     * Since XOR encryption is symmetric, this method simply calls the encrypt method.
     *
     * @param encryptedText the string to decrypt
     * @param key the decryption key (same as the encryption key)
     * @return the decrypted string
     */
    public static String decrypt(String encryptedText, String key) {
        return encrypt(encryptedText, key); // XOR encryption is its own decryption
    }

    /**
     * Encrypts a password using SHA-256 hashing and Base64 encoding.
     * This method is suitable for one-way encryption of sensitive data.
     *
     * @param password the password to encrypt
     * @return the encrypted password as a Base64 encoded string
     * @throws RuntimeException if the SHA-256 algorithm is not available
     */
    public static String encrypt(String password) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(password.getBytes());
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Splits a string into a list of strings, with each resulting string having a maximum length of 22 characters.
     * This method attempts to split the input at word boundaries (spaces) to maintain readability.
     *
     * @param string the string to split
     * @return a list of strings, each with a maximum length of 22 characters
     */
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

    /**
     * Finds the index of the nth occurrence of a character in a string.
     *
     * @param str the string to search in
     * @param target the character to search for
     * @param n the occurrence number (1-based) to find
     * @return the index of the nth occurrence of the target character, or -1 if not found
     */
    public static int nthOccurrence(String str, char target, int n) {
        int occurrence = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == target) {
                occurrence++;
                if (occurrence == n) {
                    return i;
                }
            }
        }
        return -1; // nth occurrence not found
    }

    /**
     * Converts a string to title case, where the first letter of each word is capitalized
     * and the rest of the letters are lowercase.
     * Single-character words are converted to uppercase.
     *
     * @param input the string to convert to title case
     * @return the input string converted to title case, or the original string if input is null or empty
     */
    public static String toTitleCase(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        String[] words = input.split(" ");
        StringBuilder titleCase = new StringBuilder();

        for (String word : words) {
            if (word.length() > 1) {
                titleCase.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1).toLowerCase());
            } else {
                titleCase.append(word.toUpperCase());
            }
            titleCase.append(" ");
        }

        return titleCase.toString().trim();
    }

    /**
     * Checks if a string is null or empty.
     *
     * @param str the string to check
     * @return true if the string is null or empty, false otherwise
     */
    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }
}
