package io.github.intisy.utils.utils;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public class EncryptorUtils {
    private static final String BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static String encodeBase62(byte[] input) {
        BigInteger value = new BigInteger(1, input);
        StringBuilder result = new StringBuilder();
        while (value.compareTo(BigInteger.ZERO) > 0) {
            result.append(BASE62.charAt(value.mod(BigInteger.valueOf(62)).intValue()));
            value = value.divide(BigInteger.valueOf(62));
        }

        return result.reverse().toString();
    }

    private static byte[] decodeBase62(String input) {
        BigInteger value = BigInteger.ZERO;
        for (int i = 0; i < input.length(); i++) {
            value = value.multiply(BigInteger.valueOf(62)).add(BigInteger.valueOf(BASE62.indexOf(input.charAt(i))));
        }
        return value.toByteArray();
    }
    public static String encrypt(String input, String key, String ignore) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String split : input.split(ignore)) {
            if (!first) {
                builder.append(ignore);
            }
            first = false;
            builder.append(encrypt(split, key));
        }
        return builder.toString();
    }
    public static String encode(String input, String ignore) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String split : input.split(ignore)) {
            if (!first) {
                builder.append(ignore);
            }
            first = false;
            builder.append(encodeBase62(split.getBytes()));
        }
        return builder.toString();
    }
    public static String decode(String input, String ignore) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String split : input.split(ignore)) {
            if (!first) {
                builder.append(ignore);
            }
            first = false;
            builder.append(new String(decodeBase62(split)));
        }
        return builder.toString();
    }
    public static String encrypt(String input, String key) throws Exception {
        SecretKeySpec secretKey = getSecretKey(key);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encryptedBytes = cipher.doFinal(padTo16(URLEncoder.encode(input)).getBytes());
        return encodeBase62(encryptedBytes);
    }
    public static String decrypt(String input, String key, String ignore) throws Exception {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String split : input.split(ignore)) {
            if (!first) {
                builder.append(ignore);
            }
            first = false;
            builder.append(decrypt(split, key));
        }
        return builder.toString();
    }
    public static String padTo16(String input) {
        if (input.length() >= 16) {
            return input;
        }
        StringBuilder padded = new StringBuilder(input);
        while (padded.length() < 16) {
            padded.append(' ');
        }
        return padded.toString();
    }
    public static String decrypt(String encryptedInput, String key) throws Exception {
        SecretKeySpec secretKey = getSecretKey(key);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = decodeBase62(encryptedInput);
        return stripTrailing(URLDecoder.decode(new String(cipher.doFinal(decodedBytes))));
    }

    private static String stripTrailing(String input) {
        int length = input.length();
        while (length > 0 && input.charAt(length - 1) == ' ') {
            length--;
        }
        return input.substring(0, length);
    }

    private static SecretKeySpec getSecretKey(String key) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] hashedKey = sha.digest(key.getBytes(StandardCharsets.UTF_8));
        return new SecretKeySpec(hashedKey, 0, 16, "AES");
    }
}
