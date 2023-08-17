package ai.sapper.cdc.intake.model;

import com.codekutter.common.stores.DataStoreConfig;
import com.codekutter.zconfig.common.model.annotations.ConfigValue;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.NaturalId;

import javax.annotation.Nonnull;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Objects;

@Getter
@Setter
@Entity
@Table(name = "config_mail_datastores")
public class MailDataSourceDbConfig extends DataStoreConfig {
	@ConfigValue
	@Column(name = "channel")
	private EIntakeChannel channel;
	@ConfigValue
	@Column(name = "partition_num")
	private short partition;
	@ConfigValue
	@Column(name = "mailbox")
	private String mailbox;
	@ConfigValue
	@Column(name = "smtp_connection_name")
	private String smtpConnectionName;
	@NaturalId
	@ConfigValue
	@Column(name = "connection", insertable = false)
	private String connection;

	public MailDataSourceDbConfig() {

	}

	public MailDataSourceDbConfig(@Nonnull MailDataSourceDbConfig source) {
		this.channel = source.channel;
		this.mailbox = source.mailbox;
		this.partition = source.partition;
		this.smtpConnectionName = source.smtpConnectionName;
		setAuditContextProvider(source.getAuditContextProvider());
		setAuditContextProviderClass(source.getAuditContextProviderClass());
		setAudited(source.isAudited());
		setAuditLogger(source.getAuditLogger());
		setConnectionName(source.getConnectionName());
		setConnectionType(source.getConnectionType());
		setConnectionTypeString(source.getConnectionTypeString());
		setDataStoreClass(source.getDataStoreClass());
		setDataStoreClassString(source.getDataStoreClassString());
		setDescription(source.getDescription());
		setMaxResults(source.getMaxResults());
		setName(source.getName());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		MailDataSourceDbConfig that = (MailDataSourceDbConfig) o;
		return partition == that.partition && channel == that.channel && Objects.equals(mailbox, that.mailbox)
				&& Objects.equals(smtpConnectionName, that.smtpConnectionName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(channel, partition, mailbox, smtpConnectionName);
	}
}
