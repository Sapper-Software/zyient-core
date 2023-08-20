package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.RemoteFileEntity;
import com.codekutter.common.utils.LogUtils;
import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.mail.Message;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class EmailMessage {
    private MessageHeader header;
    private String body;
    private List<String> attachments;
    private String auditedEMLFile;

    public void addAttachment(@Nonnull File file) throws IOException {
        if (attachments == null) {
            attachments = new ArrayList<>();
        }
        if (file instanceof RemoteFileEntity) {
            File lf = ((RemoteFileEntity) file).copyToLocal();
            if (lf == null || !lf.exists()) {
                throw new IOException(String.format("Error copying to local. [key=%s]",
                        ((RemoteFileEntity) file).getKey().stringKey()));
            }
            LogUtils.debug(getClass(), String.format("Copied to local instance. [key=%s][file=%s]",
                    ((RemoteFileEntity) file).getKey().stringKey(), lf.getAbsolutePath()));
        }
        attachments.add(file.getAbsolutePath());
    }

    public void addRecipient(@Nonnull String email, Message.RecipientType type) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addRecipient(address, type);
    }

    public void addReplyTo(@Nonnull String email) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addReplyTo(address);
    }

    public void addFrom(@Nonnull String email) throws AddressException {
        InternetAddress address = new InternetAddress(email);
        header.addFrom(address);
    }
}
