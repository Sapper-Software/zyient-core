package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;

@Getter
@Setter
@Embeddable
public class MailReceiptId implements IKey {
    @Column(name = "mail_id")
    private String mailId;
    @Column(name = "id")
    private String id;

    public MailReceiptId() {

    }

    public MailReceiptId(String mailId, String id) {
        this.mailId = mailId;
        this.id = id;
    }

    @Override
    public String stringKey() {
        return String.format("%s::%s", mailId, id);
    }

    @Override
    public int compareTo(IKey iKey) {
        if (iKey instanceof MailReceiptId) {
            int ret = mailId.compareTo(((MailReceiptId) iKey).mailId);
            if (ret == 0) {
                ret = id.compareTo(((MailReceiptId) iKey).id);
            }
            return ret;
        }
        return -1;
    }
}
