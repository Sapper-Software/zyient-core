package io.zyient.core.mapping.model;

import io.zyient.base.common.model.Context;
import io.zyient.base.common.model.CopyException;
import io.zyient.base.common.model.ValidationExceptions;
import io.zyient.base.common.model.entity.IEntity;
import io.zyient.base.core.model.LongKey;
import io.zyient.base.core.model.PropertyBag;
import io.zyient.core.persistence.annotations.EGeneratedType;
import io.zyient.core.persistence.annotations.GeneratedId;
import jakarta.persistence.*;
import jakarta.persistence.Column;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import javax.annotation.Generated;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Entity(name = "holdings")
@Table(name = "holdings", schema = "test")
public class Holding implements IEntity<LongKey>, PropertyBag {
    @EmbeddedId
    @Id
    @GeneratedId(type = EGeneratedType.SEQUENCE,sequence = "holding_seq")
    private LongKey id;
    @Column(name = "custodian_account_id")
    private long custodianAccountId;
    @Column(name = "quantity")
    private double quantity;
    private double price;
    @Column(name = "market_value")
    private double marketValue;
    @Column(name = "security_id")
    private Long securityId;
    @Column(name = "accured_income")
    private double accuredIncome;
    @Column(name = "transaction_type")
    private String transactionType;
    @Column(name = "trade_date")
    private int tradeDate; //YYYYMMDD
    @Transient
    private Map<String, Object> properties;


    @Override
    public int compare(LongKey key) {
        return this.id.compareTo(key);
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
        return this.id;
    }

    @Override
    public void validate() throws ValidationExceptions {

    }


    @Override
    public boolean hasProperty(@NonNull String name) {
        if (properties != null) {
            return properties.containsKey(name);
        }
        return false;
    }

    @Override
    public Object getProperty(@NonNull String name) {
        if (properties != null) {
            return properties.get(name);
        }
        return null;
    }

    @Override
    public PropertyBag setProperty(@NonNull String name, @NonNull Object value) {
        if (properties == null) {
            properties = new HashMap<>();
        }
        properties.put(name, value);
        return this;
    }
}
