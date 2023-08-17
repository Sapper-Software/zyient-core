package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.EEntityState;
import lombok.Data;

@Data
public class MailProcessedMessage {
    private EEntityState state = EEntityState.Unknown;
    private String messageId;
    private String headerJson;
    private String mailUser;
    private String mailBox;
    private long readTimestamp;
    private long processedTimestamp;
    private String s3Bucket;
    private String s3folderPath;
    private EIntakeChannel channel;
    private boolean isValidLiterature = false;
    private FileItemRecord messageFile;
}
