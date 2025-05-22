package io.github.intisy.utils.security;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Utility class providing functionality for string encryption, encoding, and decoding operations.
 * This class supports AES encryption and Base62 encoding and decoding. It also includes utility
 * methods for padding strings, generating secret keys for encryption, and handling strings with ignored delimiters.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class EncryptorUtils {
    private static final String BASE62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    /**
     * Encodes the given byte array into a Base62 encoded string.
     *
     * @param input the byte array to be encoded
     * @return a string representing the Base62 encoded value of the input byte array
     */
    private static String encodeBase62(byte[] input) {
        BigInteger value = new BigInteger(1, input);
        StringBuilder result = new StringBuilder();
        while (value.compareTo(BigInteger.ZERO) > 0) {
            result.append(BASE62.charAt(value.mod(BigInteger.valueOf(62)).intValue()));
            value = value.divide(BigInteger.valueOf(62));
        }

        return result.reverse().toString();
    }

    /**
     * Decodes a Base62 encoded string into its original byte array representation.
     *
     * @param input the Base62 encoded string to be decoded
     * @return a byte array representing the decoded value of the input string
     */
    private static byte[] decodeBase62(String input) {
        BigInteger value = BigInteger.ZERO;
        for (int i = 0; i < input.length(); i++) {
            value = value.multiply(BigInteger.valueOf(62)).add(BigInteger.valueOf(BASE62.indexOf(input.charAt(i))));
        }
        return value.toByteArray();
    }

    /**
     * Encrypts the input string using the specified encryption key and delimiter to ignore specific sections.
     * <p>
     * This method splits the input string based on the specified delimiter and encrypts each part separately,
     * then combines the results back together using the same delimiter.
     *
     * @param input the string to be encrypted
     * @param key the encryption key used for encrypting each part of the input
     * @param ignore the delimiter used to split and join the input string, sections matching this delimiter will remain unencrypted
     * @return the encrypted string with each section processed as specified
     * @throws Exception if the encryption process fails
     */
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

    /**
     * Encodes the input string by splitting it into parts based on the specified delimiter,
     * encoding each part using Base62 encoding, and rejoining them with the delimiter.
     *
     * @param input the string to be encoded
     * @param ignore the delimiter used to split and join the string
     * @return the encoded string with each part processed using Base62 encoding
     */
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

    /**
     * Decodes a Base62 encoded string by splitting it based on the specified delimiter,
     * decoding each part, and reconstructing the original string using the same delimiter.
     *
     * @param input the Base62 encoded string to be decoded
     * @param ignore the delimiter used to split and join the string during decoding
     * @return the decoded original string
     */
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

    /**
     * Encrypts the given input string using the AES encryption algorithm
     * and encodes the result into a Base62 encoded string.
     *
     * @param input the string to be encrypted
     * @param key the encryption key used for encrypting the input
     * @return the encrypted and Base62 encoded string
     * @throws Exception if an error occurs during the encryption process
     */
    public static String encrypt(String input, String key) throws Exception {
        SecretKeySpec secretKey = getSecretKey(key);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encryptedBytes = cipher.doFinal(padTo16(URLEncoder.encode(input, "UTF-8")).getBytes());
        return encodeBase62(encryptedBytes);
    }

    /**
     * Decrypts the input string by splitting it into parts based on the specified delimiter,
     * decrypting each part using the provided decryption key, and rejoining the parts with the delimiter.
     *
     * @param input the string to be decrypted
     * @param key the decryption key used for decrypting each part of the input
     * @param ignore the delimiter used to split and join the input string; sections matching this delimiter will remain unprocessed
     * @return the decrypted string with each section processed using the decryption key
     * @throws Exception if the decryption process fails
     */
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

    /**
     * Pads the input string with spaces until its length is at least 16 characters.
     * If the input string is already 16 or more characters long, it is returned as is.
     *
     * @param input the string to be padded
     * @return a string of length at least 16, padded with spaces if necessary
     */
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

    /**
     * Decrypts an encrypted input string using AES decryption
     * and decodes it from Base62 encoding.
     *
     * @param encryptedInput the string to be decrypted, encoded in Base62
     * @param key the decryption key used for decrypting the input string
     * @return the decrypted and decoded string
     * @throws Exception if an error occurs during the decryption process
     */
    public static String decrypt(String encryptedInput, String key) throws Exception {
        SecretKeySpec secretKey = getSecretKey(key);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = decodeBase62(encryptedInput);
        return stripTrailing(URLDecoder.decode(new String(cipher.doFinal(decodedBytes)), "UTF-8"));
    }

    /**
     * Removes trailing whitespace characters from the input string.
     *
     * @param input the string from which trailing spaces should be removed
     * @return the input string without trailing spaces
     */
    private static String stripTrailing(String input) {
        int length = input.length();
        while (length > 0 && input.charAt(length - 1) == ' ') {
            length--;
        }
        return input.substring(0, length);
    }

    /**
     * Generates a secret key specification using the provided key string for AES encryption.
     *
     * @param key the string used to generate the secret key
     * @return a SecretKeySpec suitable for AES encryption
     * @throws Exception if an error occurs during the key generation process
     */
    private static SecretKeySpec getSecretKey(String key) throws Exception {
        MessageDigest sha = MessageDigest.getInstance("SHA-256");
        byte[] hashedKey = sha.digest(key.getBytes(StandardCharsets.UTF_8));
        return new SecretKeySpec(hashedKey, 0, 16, "AES");
    }
}
