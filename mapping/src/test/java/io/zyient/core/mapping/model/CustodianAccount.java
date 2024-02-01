package io.zyient.core.mapping.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.LongKey;
import jakarta.persistence.*;
import jakarta.persistence.Column;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "custodian_accounts",schema = "test")
public class CustodianAccount  implements IEntity<LongKey> {
    @EmbeddedId
    private LongKey key;
    @Column(name = "custodian_id")
    private long custodianId;
    @Column(name = "account_no")
    private String accountNo;
    @Column(name = "account_name")
    private String accountName;
    @Column(name = "fund_name")
    private String fundName;
    @Column(name = "investment_strategy")
    private String investmentStrategy;
    @Column(name = "connectivity_date")
    private int connectivityDate;
    /**
     * if Custodian.connectivityType is BOTH or DAILY_FEED then feedStartDate is enabled on UI.
     */
    @Column(name = "feed_start_date")
    private int feedStartDate;
    @Column(name = "start_date")
    private int startDate;
    @Column(name = "inception_date")
    private int inceptionDate;

    @Override
    public int compare(LongKey key) {
        return this.key.compareTo(key);
    }

    @Override
    public IEntity<LongKey> copyChanges(IEntity<LongKey> source, Context context) throws CopyException {
        return null;
    }

    @Override
    public IEntity<LongKey> clone(Context context) throws CopyException {
        return null;
    }

    @Override
    public LongKey entityKey() {
        return key;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }
}
