package ai.sapper.cdc.intake.model;

import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class EmailJsonWrapper {
    private String senderName;
    private String senderEmail;
    private MessageHeaderWrapper header;
    private List<String> toGroups;
    private List<String> ccGroups;
    private List<String> bccGroups;
    private List<BaseAttachmentWrapper> attachments;
    private String template;
    private Map<String, String> context;
    private String body;

    public void addAttachment(@Nonnull BaseAttachmentWrapper attachment) {
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
