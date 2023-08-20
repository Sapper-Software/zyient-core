package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.common.model.Context;
import ai.sapper.cdc.common.model.CopyException;
import ai.sapper.cdc.common.model.ValidationExceptions;
import ai.sapper.cdc.common.model.entity.IEntity;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Getter
@Setter
@Entity
@Table(name = "config_mail_template_params")
public class MailTemplateParam implements IEntity<MailTemplateParamId> {
    @EmbeddedId
    private MailTemplateParamId id;
    @Column(name = "is_mandatory")
    private boolean mandatory;
    @Column(name = "default_value")
    private String defaultValue;

    @Override
    public int compare(MailTemplateParamId mailTemplateParamId) {
        return id.compareTo(mailTemplateParamId);
    }

    @Override
    public IEntity<MailTemplateParamId> copyChanges(IEntity<MailTemplateParamId> iEntity, Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IEntity<MailTemplateParamId> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public MailTemplateParamId getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
