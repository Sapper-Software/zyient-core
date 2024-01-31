package io.zyient.core.mapping.readers.impl.separated.encrypted;

import io.zyient.core.mapping.readers.impl.separated.SeparatedInputReader;
import io.zyient.core.mapping.readers.settings.EncryptedSeparatedReaderSettings;
import io.zyient.core.mapping.readers.util.PgpDecryptionUtil;

import java.io.*;

public class EncryptedSeparatedInputReader extends SeparatedInputReader {

    @Override
    protected Reader getReader(File file) throws IOException {
        try {
            InputStream stream = new FileInputStream(contentInfo().path());
            InputStream privateKeyStream = new FileInputStream(((EncryptedSeparatedReaderSettings) settings()).getDecryptionKeyName());
            String password = fetchPassword(((EncryptedSeparatedReaderSettings) settings()).getDecryptionSecret());
            PgpDecryptionUtil pgpDecryptionUtil = new PgpDecryptionUtil(privateKeyStream, password);
            stream = pgpDecryptionUtil.decryptInputStream(stream);
            return new InputStreamReader(stream);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private String fetchPassword(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                return line;
            }
        }
        throw new IOException("Password not found in the file.");
    }
}
