package ai.sapper.cdc.intake.model;

import jakarta.mail.Address;
import jakarta.mail.Message;
import jakarta.mail.internet.InternetAddress;
import lombok.Data;

import javax.annotation.Nonnull;

@Data
public class MessageHeader {
    private String id;
    private String[] from;
    private String[] to;
    private String[] cc;
    private String[] bcc;
    private String[] replyTo;
    private long sendDate;
    private long receivedDate;
    private String subject;

    public void addRecipient(@Nonnull Address address, Message.RecipientType type) {
        if (address instanceof InternetAddress) {
            if (type == Message.RecipientType.TO) {
                to = copyTo(to, address);
            } else if (type == Message.RecipientType.CC) {
                cc = copyTo(cc, address);
            } else if (type == Message.RecipientType.BCC) {
                bcc = copyTo(bcc, address);
            }
        }
    }

    public void addReplyTo(@Nonnull Address address) {
        if (address instanceof InternetAddress)
            replyTo = copyTo(replyTo, address);
    }

    public void addFrom(@Nonnull Address address) {
        if (address instanceof InternetAddress)
            from = copyTo(from, address);
    }

    private String[] copyTo(String[] source, Address address) {
        if (source == null) {
            source = new String[1];
            source[0] = address.toString();
            return source;
        } else {
            String[] array = new String[source.length + 1];
            for (int ii = 0; ii < source.length; ii++) {
                array[ii] = source[ii];
            }
            array[array.length - 1] = address.toString();
            return array;
        }
    }
}
