package ai.sapper.cdc.intake.utils;

import ai.sapper.cdc.intake.flow.FlowTaskException;
import ai.sapper.cdc.intake.model.AbstractMailMessage;
import ai.sapper.cdc.intake.model.EIntakeChannel;
import ai.sapper.cdc.intake.model.MailReceiptRecord;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import javax.annotation.Nonnull;

public class MailAuditDataUtils {

	public static MailReceiptRecord create(@Nonnull EIntakeChannel channel,
										   @Nonnull AbstractMailMessage<?> message,
										   @Nonnull String callerId) throws FlowTaskException {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(callerId));
		Preconditions.checkArgument(message.getMessage() != null);

		try {
			MailReceiptRecord record = new MailReceiptRecord();
			record.setMessageId(new MailReceiptId(message.getMailId(), message.getMessageId().getKey()));
			record.setChannel(channel);
			record.setProcessorId(callerId);
			record.setReadTimestamp(System.currentTimeMillis());
			if (message.getHeader() == null) {
				MessageHeader header = MailUtils.parseHeader(message);
				message.setHeader(header);
			}
			record.setHeaderJson(MailUtils.generateHeaderJson(message.getHeader()));
			record.setReceivedTimestamp(message.getHeader().getReceivedDate());
			record.setMessageHash(MailUtils.generateMessageHash(MailUtils.parseHeader(message)));

			return record;
		} catch (Exception ex) {
			throw new FlowTaskException(ex);
		}
	}

	public static MailMetaDataRecord create(@Nonnull AbstractMailMessage<?> message,
										   @Nonnull String callerId) throws FlowTaskException {
		Preconditions.checkArgument(!Strings.isNullOrEmpty(callerId));
		Preconditions.checkArgument(message.getMessage() != null);

		try {
			MailMetaDataRecord record = new MailMetaDataRecord();
			record.setMessageId(new MailReceiptId(message.getMailId(), message.getMessageId().getKey()));
			if (message.getHeader() == null) {
				MessageHeader header = MailUtils.parseHeader(message);
				message.setHeader(header);
			}
			record.setHeaderJson(MailUtils.generateHeaderJson(message.getHeader()));
			record.setReceivedTimestamp(message.getHeader().getReceivedDate());
			record.setProcessedTimestamp(System.currentTimeMillis());
			return record;
		} catch (Exception ex) {
			throw new FlowTaskException(ex);
		}
	}

	public static MessageHeader getHeader(@Nonnull MailReceiptRecord record) throws Exception {
		String json = record.getHeaderJson();
		if (Strings.isNullOrEmpty(json)) {
			throw new FlowTaskException("NULL/Empty header JSON data.");
		}
		MessageHeader header = GlobalConstants.getJsonMapper().readValue(json, MessageHeader.class);
		LogUtils.debug(MailAuditDataUtils.class, header);

		return header;
	}
}
