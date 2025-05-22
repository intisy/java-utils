package io.github.intisy.utils.security;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.spec.KeySpec;
import java.util.Base64;

/**
 * Utility class for secure password encryption and decryption operations.
 * This class provides methods for AES encryption with password-based key derivation,
 * salt generation, and initialization vector (IV) generation.
 * <p>
 * The implementation uses PBKDF2 with HMAC-SHA256 for key derivation, AES in CBC mode
 * with PKCS5 padding for encryption, and Base64 encoding for the encrypted output.
 *
 * @author Finn Birich
 */
@SuppressWarnings("unused")
public class PasswordEncryptionUtil {

    /**
     * Derives a secret key from a password and salt using PBKDF2 with HMAC-SHA256.
     * This method uses 65536 iterations and generates a 256-bit key suitable for AES encryption.
     *
     * @param password the password to derive the key from
     * @param salt the salt to use in the key derivation process
     * @return a SecretKeySpec that can be used for AES encryption/decryption
     * @throws Exception if the key derivation process fails
     */
    public static SecretKeySpec getKeyFromPassword(String password, byte[] salt) throws Exception {
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, 65536, 256); // 65536 iterations, 256-bit key
        byte[] key = factory.generateSecret(spec).getEncoded();
        return new SecretKeySpec(key, "AES");
    }

    /**
     * Generates a random salt suitable for use in password-based encryption.
     * The salt is 16 bytes (128 bits) in length, which is the AES block size.
     *
     * @return a randomly generated salt as a byte array
     */
    public static byte[] generateSalt() {
        byte[] salt = new byte[16]; // AES block size
        new SecureRandom().nextBytes(salt);
        return salt;
    }

    /**
     * Generates a random initialization vector (IV) suitable for use in AES encryption.
     * The IV is 16 bytes (128 bits) in length, which is the AES block size.
     *
     * @return a randomly generated IV as an IvParameterSpec
     */
    public static IvParameterSpec generateIv() {
        byte[] iv = generateSalt();
        return new IvParameterSpec(iv);
    }

    /**
     * Encrypts a plaintext string using AES encryption in CBC mode with PKCS5 padding.
     * The encryption uses a key derived from the provided password and salt,
     * and the specified initialization vector.
     *
     * @param plainText the text to encrypt
     * @param password the password to derive the encryption key from
     * @param salt the salt to use in the key derivation process
     * @param iv the initialization vector to use for the encryption
     * @return the encrypted text as a Base64-encoded string
     * @throws Exception if the encryption process fails
     */
    public static String encrypt(String plainText, String password, byte[] salt, IvParameterSpec iv) throws Exception {
        SecretKeySpec key = getKeyFromPassword(password, salt);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key, iv);
        byte[] cipherText = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(cipherText);
    }

    /**
     * Decrypts a ciphertext string that was encrypted using AES encryption in CBC mode with PKCS5 padding.
     * The decryption uses a key derived from the provided password and salt,
     * and the specified initialization vector.
     *
     * @param cipherText the Base64-encoded encrypted text to decrypt
     * @param password the password to derive the decryption key from
     * @param salt the salt used in the key derivation process
     * @param iv the initialization vector used for the encryption
     * @return the decrypted plaintext string
     * @throws Exception if the decryption process fails
     */
    public static String decrypt(String cipherText, String password, byte[] salt, IvParameterSpec iv) throws Exception {
        SecretKeySpec key = getKeyFromPassword(password, salt);
        Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key, iv);
        byte[] plainText = cipher.doFinal(Base64.getDecoder().decode(cipherText));
        return new String(plainText, StandardCharsets.UTF_8);
    }
}
