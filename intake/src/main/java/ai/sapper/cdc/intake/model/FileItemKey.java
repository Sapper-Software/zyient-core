package ai.sapper.cdc.intake.model;

import ai.sapper.cdc.common.model.entity.IKey;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Getter
@Setter
@Embeddable
public class FileItemKey implements IKey, Serializable {
    @Column(name = "bucket")
    private String bucket;
    @Column(name = "path")
    private String path;

    @Override
    public String stringKey() {
        return String.format("%s:%s", bucket, path);
    }

    @Override
    public int compareTo(IKey iKey) {
        int ret = -1;
        if (iKey instanceof FileItemKey) {
            ret = bucket.compareTo(((FileItemKey) iKey).bucket);
            if (ret == 0) {
                ret = path.compareTo(((FileItemKey) iKey).path);
            }
        }
        return ret;
    }
}
