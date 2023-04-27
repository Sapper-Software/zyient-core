package ai.sapper.cdc.core.io;

import ai.sapper.cdc.common.utils.JSONUtils;
import ai.sapper.cdc.common.utils.PathUtils;
import ai.sapper.cdc.core.connections.ZookeeperConnection;
import ai.sapper.cdc.core.io.model.EFileState;
import ai.sapper.cdc.core.io.model.FileInode;
import ai.sapper.cdc.core.io.model.Inode;
import ai.sapper.cdc.core.io.model.InodeType;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;

public class FSStateManager {
    private final ZookeeperConnection connection;
    private final String zkRootPath;

    public FSStateManager(@NonNull ZookeeperConnection connection,
                          @NonNull String zkRootPath) {
        this.connection = connection;
        this.zkRootPath = zkRootPath;
    }

    public void create(@NonNull Inode inode) throws IOException {
        if (inode.getType() == InodeType.Temp) {
            return;
        }
        try {
            PathUtils.ZkPathBuilder builder = zkPath(inode);
            String zkPath = builder.build();
            CuratorFramework client = connection.client();
            if (client.checkExists().forPath(zkPath) != null) {
                byte[] data = client.getData().forPath(zkPath);
                if (data != null && data.length > 0) {
                    Inode node = JSONUtils.read(data, inode.getClass());
                    if (node.getType() != inode.getType()) {
                        throw new IOException(String.format("[%s] Cannot change inode type. [current=%s][create=%s]",
                                zkPath,
                                node.getType().name(),
                                inode.getType().name()));
                    }
                    if (node.getType() == InodeType.File) {
                        FileInode fi = (FileInode) node;
                        if (fi.getState() != EFileState.Deleted) {
                            throw new IOException(String.format("[%s] Already exists.", zkPath));
                        }
                    }
                }
            } else {
                client.create().creatingParentContainersIfNeeded().forPath(zkPath);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private void createParentsIfRequired(@NonNull Inode inode) throws Exception {
        if (inode.getParent() != null) {
            createParentsIfRequired(inode.getParent());
        }

    }

    private PathUtils.ZkPathBuilder zkPath(Inode inode) {
        PathUtils.ZkPathBuilder path = null;
        if (inode.getParent() != null) {
            path = zkPath(inode.getParent());
        }
        if (path == null) {
            path = new PathUtils.ZkPathBuilder(zkRootPath);
        }
        path.withPath(inode.getName());
        return path;
    }
}
