package ai.sapper.cdc.core.io.impl;

import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import java.io.IOException;

@Getter
@Accessors(fluent = true)
public abstract class CDCFileSystem extends FileSystem {

}
