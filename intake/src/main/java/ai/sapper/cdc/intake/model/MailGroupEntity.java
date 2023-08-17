package ai.sapper.cdc.intake.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nonnull;
import javax.persistence.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "config_mail_groups")
public class MailGroupEntity implements IEntity<IdKey> {
    @EmbeddedId
    private IdKey id;
    @Transient
    @Setter(AccessLevel.NONE)
    private Set<String> recipients;
    @Column(name = "recipients")
    @Setter(AccessLevel.NONE)
    @JsonIgnore
    private String recipientList;

    public void setRecipientList(String recipientList) {
        this.recipientList = recipientList;
        if (!Strings.isNullOrEmpty(recipientList)) {
            recipients = new HashSet<>();
            String[] parts = recipientList.split(";");
            if (parts != null && parts.length > 0) {
                recipients.addAll(Arrays.asList(parts));
            }
        } else {
            recipients = null;
        }
    }

    public void setRecipients(Set<String> recipients) {
        this.recipients = recipients;
        if (recipients != null) {
            StringBuilder builder = new StringBuilder();
            boolean first = true;
            for (String r : recipients) {
                if (first) first = false;
                else {
                    builder.append(";");
                }
                builder.append(r);
            }
            recipientList = builder.toString();
        }
    }

    public void addRecipient(@Nonnull String recipient) {
        if (recipients == null) {
            recipients = new HashSet<>();
        }
        recipients.add(recipient);
        if (!Strings.isNullOrEmpty(recipientList)) {
            recipientList = String.format("%s;%s", recipientList, recipient);
        } else
            recipientList = recipient;
    }

    public boolean removeRecipient(@Nonnull String recipient) {
        if (recipients != null) {
            boolean ret = recipients.remove(recipient);
            if (ret) {
                StringBuilder builder = new StringBuilder();
                boolean first = true;
                for (String r : recipients) {
                    if (first) first = false;
                    else {
                        builder.append(";");
                    }
                    builder.append(r);
                }
                recipientList = builder.toString();
            }
            return ret;
        }
        return false;
    }

    public void clearRecipients() {
        if (recipients != null) recipients.clear();
        recipientList = null;
    }

    @Override
    public int compare(IdKey idKey) {
        return id.compareTo(idKey);
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<IdKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public IdKey getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
