package io.zyient.core.mapping.readers.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class PasswordUtil {
    public static String fetchPassword(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                return line;
            }
        }
        throw new IOException("Password not found in the file.");
    }
}
