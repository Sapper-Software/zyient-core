package ai.sapper.cdc.intake.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.mail.internet.InternetAddress;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public class EmailJson {
    private String senderName;
    private InternetAddress senderEmail;
    private MessageHeader header;
    private List<String> toGroups;
    private List<String> ccGroups;
    private List<String> bccGroups;
    private List<BaseAttachment> attachments;

    public void addAttachment(@Nonnull BaseAttachment attachment) {
        if (attachments == null) {
            attachments = new ArrayList<>();
        }
        attachments.add(attachment);
    }

    public void addToGroup(@Nonnull String mailGroup) {
        if (toGroups == null) {
            toGroups = new ArrayList<>();
        }
        toGroups.add(mailGroup);
    }

    public void addCcGroup(@Nonnull String mailGroup) {
        if (ccGroups == null) {
            ccGroups = new ArrayList<>();
        }
        ccGroups.add(mailGroup);
    }
    public void addBccGroup(@Nonnull String mailGroup) {
        if (bccGroups == null) {
            bccGroups = new ArrayList<>();
        }
        bccGroups.add(mailGroup);
    }
}
