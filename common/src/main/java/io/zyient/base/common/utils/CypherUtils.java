/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.base.common.utils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.GlobalConstants;
import lombok.NonNull;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.Console;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

public class CypherUtils {
    public static final String HASH_ALGO = "MD5";
    public static final String CIPHER_ALGO = "AES/CBC/PKCS5Padding";
    public static final String CIPHER_TYPE = "AES";
    @Parameter(names = {"-h", "--hash"}, description = "Get the MD5 Hash")
    private boolean doHash = false;
    @Parameter(names = {"-e", "--encrypt"}, description = "Encrypt the passed String")
    private boolean encrypt = false;
    @Parameter(names = {"-d", "--decrypt"}, description = "Decrypt the passed String")
    private boolean decrypt = false;
    @Parameter(names = {"-p", "--password"}, description = "Password used to encrypt/decrypt")
    private String password;
    @Parameter(names = {"-i", "--IV"}, description = "IV Spec used to encrypt/decrypt")
    private String ivSpec;
    @Parameter(names = {"--help"}, help = true)
    private boolean help = false;
    @Parameter(description = "Other program arguments")
    private List<String> otherArgs = new ArrayList<>();

    public static String checkPassword(@Nonnull String password, @Nonnull String name) throws Exception {
        String pwd = String.format("%s--%s", name, password);
        byte[] buff = pwd.getBytes(StandardCharsets.UTF_8);
        if (buff.length < 16 ) {
            throw new Exception(
                    String.format("Invalid password. expected lengths=(16, 24, 32). [length=%d]", buff.length));
        }
        if (buff.length > 32) {
            pwd = pwd.substring(0, 32);
        } else if (buff.length > 24) {
            pwd = pwd.substring(0, 24);
        } else if (buff.length > 16) {
            pwd = pwd.substring(0, 16);
        }
        return pwd;
    }

    public static String formatIvString(@NonNull String iv) throws Exception {
        byte[] buff = iv.getBytes(StandardCharsets.UTF_8);
        if (buff.length > 16) {
            iv = new String(buff, 0, 16, StandardCharsets.UTF_8);
        } else if (buff.length < 16) {
            throw new Exception(String.format("Invalid IV Spec string, less than 16 bytes. [value=%s]", iv));
        }
        return iv;
    }

    /**
     * Get an MD5 hash of the specified key.
     *
     * @param key - Input Key.
     * @return - String value of the Hash
     * @throws Exception
     */
    public static String getKeyHash(@Nonnull String key) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(key));

        MessageDigest digest = MessageDigest.getInstance(HASH_ALGO);
        byte[] d = digest.digest(key.getBytes(GlobalConstants.defaultCharset()));
        d = Base64.encodeBase64(d);
        return new String(d, GlobalConstants.defaultCharset());
    }

    /**
     * Get an MD5 hash of the specified byte array.
     *
     * @param data - Input byte array.
     * @return - String value of the Hash
     * @throws Exception
     */
    public static String getHash(@Nonnull byte[] data) throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);

        MessageDigest digest = MessageDigest.getInstance(HASH_ALGO);
        byte[] d = digest.digest(data);
        d = Base64.encodeBase64(d);
        return new String(d, GlobalConstants.defaultCharset());
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Encrypted Buffer.
     * @throws Exception
     */
    public static byte[] encrypt(@Nonnull String algo,
                                 @Nonnull byte[] data,
                                 @Nonnull String password,
                                 @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        Cipher cipher = getCipher(algo, password, iv, Cipher.ENCRYPT_MODE);

        return cipher.doFinal(data);
    }

    public static byte[] encrypt(@Nonnull byte[] data,
                                 @Nonnull String password,
                                 @Nonnull String iv)
            throws Exception {
        return encrypt(CIPHER_ALGO, data, password, iv);
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Base64 encoded String.
     * @throws Exception
     */
    public static String encryptAsString(@Nonnull String algo,
                                         @Nonnull byte[] data,
                                         @Nonnull String password,
                                         @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] encrypted = encrypt(algo, data, password, iv);
        return new String(Base64.encodeBase64(encrypted));
    }

    public static String encryptAsString(@Nonnull byte[] data,
                                         @Nonnull String password,
                                         @Nonnull String iv)
            throws Exception {
        return encryptAsString(CIPHER_ALGO, data, password, iv);
    }

    /**
     * Encrypt the passed data buffer using the passcode.
     *
     * @param data     - Data Buffer.
     * @param password - Passcode.
     * @param iv       - IV Key
     * @return - Base64 encoded String.
     * @throws Exception
     */
    public static String encryptAsString(@Nonnull String algo,
                                         @Nonnull String data,
                                         @Nonnull String password,
                                         @Nonnull String iv)
            throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] encrypted = encrypt(algo, data.getBytes(GlobalConstants.defaultCharset()), password, iv);
        return new String(Base64.encodeBase64(encrypted));
    }

    public static String encryptAsString(@Nonnull String data,
                                         @Nonnull String password,
                                         @Nonnull String iv)
            throws Exception {
        return encryptAsString(CIPHER_ALGO, data, password, iv);
    }

    /**
     * Decrypt the data buffer using the passcode.
     *
     * @param data     - Encrypted Data buffer.
     * @param password - Passcode
     * @param iv       - IV Key
     * @return - Decrypted Data Buffer.
     * @throws Exception
     */
    public static byte[] decrypt(@Nonnull String algo,
                                 @Nonnull byte[] data,
                                 @Nonnull String password,
                                 @Nonnull String iv) throws Exception {
        Preconditions.checkArgument(data != null && data.length > 0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        Cipher cipher = getCipher(algo, password, iv, Cipher.DECRYPT_MODE);
        // decrypt the text

        return cipher.doFinal(data);
    }

    public static byte[] decrypt(@Nonnull byte[] data,
                                 @Nonnull String password,
                                 @Nonnull String iv) throws Exception {
        return decrypt(CIPHER_ALGO, data, password, iv);
    }

    public static Cipher getCipher(@Nonnull String password,
                                   @Nonnull String iv,
                                   int mode) throws Exception {
        return getCipher(CIPHER_ALGO, password, iv, mode);
    }

    public static Cipher getCipher(@Nonnull String algo,
                                   @Nonnull String password,
                                   @Nonnull String iv,
                                   int mode) throws Exception {
        // Create key and cipher
        Key aesKey = new SecretKeySpec(password.getBytes(GlobalConstants.defaultCharset()), CIPHER_TYPE);
        IvParameterSpec ivspec = new IvParameterSpec(iv.getBytes(GlobalConstants.defaultCharset()));

        Cipher cipher = Cipher.getInstance(algo);
        cipher.init(mode, aesKey, ivspec);

        return cipher;
    }

    /**
     * Decrypt the string data using the passcode.
     *
     * @param data     - Encrypted String data.
     * @param password - Passcode
     * @param iv       - IV Key
     * @return - Decrypted Data Buffer.
     * @throws Exception
     */
    public static byte[] decrypt(@Nonnull String data, @Nonnull String password, @Nonnull String iv) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(data));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(password));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(iv));

        byte[] array = Base64.decodeBase64(data);
        return decrypt(array, password, iv);
    }

    public static void main(String[] args) {
        try {
            new CypherUtils().execute(args);
        } catch (Throwable t) {
            DefaultLogger.error(CypherUtils.class.getCanonicalName(), t);
            DefaultLogger.stacktrace(t);
        }
    }

    private void execute(String[] args) throws Exception {
        JCommander parser = JCommander.newBuilder().addObject(this).build();
        // if you have a wider console, you could increase the value;
        // here 80 is also the default

        String value = null;
        try {
            // parse the arguments.
            parser.parse(args);

            if (help) {
                parser.usage();
                return;
            }
            // you can parse additional arguments if you want.
            // parser.parseArgument("more","args");

            // after parsing arguments, you should check
            // if enough arguments are given.
            if (otherArgs.isEmpty())
                throw new ParameterException("No program arguments given...");
            value = otherArgs.get(0);
            if (Strings.isNullOrEmpty(value)) {
                throw new ParameterException("NULL/Empty value to Hash/Encrypt.");
            }

        } catch (ParameterException e) {
            parser.usage();
            throw e;
        }
        if (encrypt) {
            String pwd = getPassword();
            String output =
                    encryptAsString(value.getBytes(GlobalConstants.defaultCharset()), pwd, ivSpec);
            System.out.println(String.format("Encrypted Text: %s", output));
        } else if (decrypt) {
            String pwd = getPassword();
            byte[] buff = decrypt(value.getBytes(GlobalConstants.defaultCharset()), pwd, ivSpec);
            String output = new String(buff, GlobalConstants.defaultCharset());
            System.out.println(String.format("Decrypted Text: %s", output));
        } else if (doHash) {
            String output = getKeyHash(value);
            System.out.println(String.format("MD5 Hash: %s", output));
        } else {
            Exception e = new Exception("No valid option set.");
            parser.usage();
            throw e;
        }
    }

    private String getPassword() {
        if (Strings.isNullOrEmpty(password)) {
            Console console = System.console();
            while (true) {
                char[] buff = console.readPassword("Enter Password:");
                if (buff == null || buff.length == 0) {
                    continue;
                }
                if (buff.length != 16) {
                    System.err.println("Invalid Password : Must be 16 characters.");
                    continue;
                }
                password = new String(buff);
                break;
            }
        }
        return password;
    }
}
