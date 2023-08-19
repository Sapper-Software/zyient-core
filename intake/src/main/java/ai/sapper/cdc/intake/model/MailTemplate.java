package ai.sapper.cdc.intake.model;

import com.codekutter.common.Context;
import com.codekutter.common.model.CopyException;
import com.codekutter.common.model.IEntity;
import com.codekutter.common.model.ValidationExceptions;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;

@Getter
@Setter
@Entity
@Table(name = "config_mail_templates")
public class MailTemplate implements IEntity<IdKey> {
    @EmbeddedId
    private IdKey id;
    @Column(name = "template")
    private String template;
    @Enumerated(EnumType.STRING)
    @Column(name = "template_type")
    private ETemplateType templateType;
    @Enumerated(EnumType.STRING)
    @Column(name = "template_source")
    private EContentSource templateSource;
    @Column(name = "updated_by")
    private String updatedBy;
    @Column(name = "update_timestamp")
    private long updateTimestamp;
    @OneToMany(fetch = FetchType.EAGER)
    @JoinColumn(name = "template_id")
    private Set<MailTemplateParam> params;


    @Override
    public int compare(IdKey idKey) {
        return id.compareTo(idKey);
    }

    @Override
    public IEntity<IdKey> copyChanges(IEntity<IdKey> iEntity, Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IEntity<IdKey> clone(Context context) throws CopyException {
        throw new CopyException("Method not implemented.");
    }

    @Override
    public IdKey getKey() {
        return id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
