package ai.sapper.cdc.core.io.model;

import ai.sapper.cdc.core.io.impl.local.LocalPathInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.io.File;
import java.io.IOException;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class TempFile extends Inode {

    public TempFile(@NonNull LocalPathInfo path,
                    @NonNull String name) {
        super(InodeType.Temp, name);
        Preconditions.checkNotNull(path.file());
        setPath(path);
    }

    @Override
    public boolean exists() {
        try {
            return getPath().exists();
        } catch (IOException ex) {
            return false;
        }
    }
}
