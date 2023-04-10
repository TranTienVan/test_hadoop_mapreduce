package Section07;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class EncryptModule {
    private static final String ALGORITHM = "AES";
    private static final String KEY = "MySecretKey12345"; // Replace with your own secret key

    public String encrypt(String input) {
        try {
            SecretKeySpec keySpec = new SecretKeySpec(KEY.getBytes(), ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
            byte[] encryptedBytes = cipher.doFinal(input.getBytes());
            return Base64.getEncoder().encodeToString(encryptedBytes);
        } catch (Exception e) {
            System.err.println("Error encrypting string: " + e.getMessage());
            return null;
        }
    }

    // public static void main(String[] args) throws Exception {
    // String originalString = "This is a secret message";
    // String encryptedString = encrypt(originalString);

    // System.out.println("Original string: " + originalString);
    // System.out.println("Encrypted string: " + encryptedString);
    // }

}
