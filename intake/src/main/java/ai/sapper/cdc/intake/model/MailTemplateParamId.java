package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@Embeddable
public class MailTemplateParamId implements IKey, Serializable {
    @Column(name = "template_id")
    private String templateId;
    @Column(name = "name")
    private String key;

    @Override
    public String stringKey() {
        return String.format("%s::%s", templateId, key);
    }

    @Override
    public int compareTo(IKey iKey) {
        if (iKey instanceof MailTemplateParamId) {
            int ret = templateId.compareTo(((MailTemplateParamId) iKey).templateId);
            if (ret == 0) {
                ret = key.compareTo(((MailTemplateParamId) iKey).key);
            }
            return ret;
        }
        return -1;
    }
}
