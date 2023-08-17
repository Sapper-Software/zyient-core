package ai.sapper.cdc.intake.model;

import com.codekutter.common.model.EEntityState;
import lombok.Data;

@Data
public class FileProcessedMessage {
    private EEntityState state = EEntityState.Unknown;
    private String messageId;
    private String uploadBucket;
    private String uploadPath;
    private String sourceBucket;
    private String sourceFileKey;
    private String sourceRootPath;
    private long sourceTimestamp;
    private long processedTimestamp;
    private EIntakeChannel channel;
    private boolean isValidLiterature = false;
    private FileItemRecord file;
    private boolean isPST = false;
}
