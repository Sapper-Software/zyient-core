/*
 * Copyright(C) (2024) Zyient Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zyient.core.filesystem.indexing;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.JSONUtils;
import io.zyient.base.common.utils.PathUtils;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.DistributedLock;
import io.zyient.base.core.connections.common.ZookeeperConnection;
import io.zyient.base.core.index.IndexBuilder;
import io.zyient.base.core.index.SearchCursor;
import io.zyient.core.filesystem.FileSystem;
import io.zyient.core.filesystem.impl.PostOperationVisitor;
import io.zyient.core.filesystem.model.EFileState;
import io.zyient.core.filesystem.model.FileInode;
import io.zyient.core.filesystem.model.Inode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Getter
@Accessors(fluent = true)
public class FileSystemIndexer implements Closeable, PostOperationVisitor {
    private FileSystemIndexerSettings settings;
    private FileSystem fs;
    private BaseEnv<?> env;
    private final Analyzer analyzer = new StandardAnalyzer();
    private final IndexBuilder builder = new IndexBuilder();
    private IndexWriter writer;
    private IndexSearcher search;
    private Directory baseDir;
    private ExecutorService executor;
    private ZookeeperConnection connection;
    private DistributedLock indexLock;
    private String zkBasePath;
    private FileIndexerState state;

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     @NonNull BaseEnv<?> env,
                     @NonNull FileSystem fs) throws ConfigurationException {
        this.env = env;
        this.fs = fs;
        ConfigReader reader = new ConfigReader(config, FileSystemIndexerSettings.class);
        reader.read();
        settings = (FileSystemIndexerSettings) reader.settings();
        setup();
    }

    public void init(@NonNull FileSystemIndexerSettings settings,
                     @NonNull BaseEnv<?> env,
                     @NonNull FileSystem fs) throws ConfigurationException {
        this.settings = settings;
        this.env = env;
        this.fs = fs;
        setup();
    }

    private void setup() throws ConfigurationException {
        try {
            connection = env.connectionManager().getConnection(settings().getZkConnection(), ZookeeperConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("ZooKeeper Connection not found. [name=%s]",
                                settings.getZkConnection()));
            }
            zkBasePath = new PathUtils.ZkPathBuilder(fs.zkPath())
                    .withPath("indexer")
                    .build();

            indexLock = env.createLock(getLockPath(), "fs", fs.settings().getName());
            indexLock.lock();
            try {
                state = checkAndGetState();
                File dir = new File(settings.getDirectory());
                if (!dir.exists()) {
                    if (!dir.mkdirs()) {
                        throw new IOException(String.format("Failed to create directory. [path=%s]", settings.getDirectory()));
                    }
                }
                if (settings().isUseMappedFiles()) {
                    baseDir = new MMapDirectory(dir.toPath());
                } else {
                    baseDir = new NIOFSDirectory(dir.toPath());
                }
                executor = Executors.newFixedThreadPool(settings().getPoolSize());
                IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
                writer = new IndexWriter(baseDir, indexWriterConfig);
                if (!state.initialized()) {
                    state.setState(EFileIndexerState.Initialized);
                    state = save(state);
                }
                search = new IndexSearcher(DirectoryReader.open(writer), executor);
            } finally {
                indexLock.unlock();
            }
        } catch (Exception ex) {
            if (state != null) {
                state.error(ex);
            }
            throw new ConfigurationException(ex);
        }
    }

    private FileIndexerState checkAndGetState() throws Exception {
        FileIndexerState state = getState();
        if (state == null) {
            state = new FileIndexerState();
            state.setFsName(fs.settings().getName());
            state.setFsZkPath(fs.zkPath());
            state.setCount(0);
            state.setTimeCreated(System.currentTimeMillis());
            state.setTimeUpdated(System.currentTimeMillis());
        }
        return state;
    }

    private FileIndexerState save(FileIndexerState state) throws Exception {
        state.setTimeUpdated(System.currentTimeMillis());
        String json = JSONUtils.asString(state);
        String path = getStatePath();
        CuratorFramework client = connection.client();
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }
        client.setData().forPath(path, json.getBytes(StandardCharsets.UTF_8));
        return state;
    }

    private FileIndexerState getState() throws Exception {
        String path = getStatePath();
        CuratorFramework client = connection().client();
        if (client.checkExists().forPath(path) != null) {
            return JSONUtils.read(client, path, FileIndexerState.class);
        }
        return null;
    }

    private String getStatePath() {
        return new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath("state")
                .build();
    }

    private String getLockPath() {
        return new PathUtils.ZkPathBuilder(zkBasePath)
                .withPath("__lock")
                .build();
    }

    public void index(@NonNull FileInode inode) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        indexLock.lock();
        try {
            state = getState();
            Document doc = builder.build(inode);
            writer.addDocument(doc);
            writer.commit();
            if (inode.getState().getState() == EFileState.New) {
                state.setCount(state().getCount() + 1);
                state = save(state);
            }
        } finally {
            indexLock.unlock();
        }
    }

    public boolean delete(@NonNull FileInode inode) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        indexLock.lock();
        try {
            state = getState();
            Document doc = findById(inode.getUuid());
            if (doc != null) {
                long ret = writer.deleteDocuments(
                        new Term(InodeIndexConstants.NAME_UUID, String.format("\"%s\"", inode.getUuid())));
                if (ret > 1) {
                    throw new Exception(
                            String.format("Index corrupted: multiple document deleted. [count=%s][uuid=%s]",
                                    ret, inode.getUuid()));
                }
                if (ret == 1) {
                    state.setCount(state().getCount() - 1);
                    state = save(state);
                }
                return (ret == 1);
            }
            return false;
        } finally {
            indexLock.unlock();
        }
    }

    public boolean delete(@NonNull String fsPath) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        indexLock.lock();
        try {
            state = getState();
            Document doc = findByFsPath(fsPath);
            if (doc != null) {
                long ret = writer.deleteDocuments(
                        new Term(InodeIndexConstants.NAME_FS_PATH, String.format("\"%s\"", fsPath)));
                if (ret > 1) {
                    throw new Exception(
                            String.format("Index corrupted: multiple document deleted. [count=%s][faPath=%s]",
                                    ret, fsPath));
                }
                if (ret == 1) {
                    state.setCount(state().getCount() - 1);
                    state = save(state);
                }
                return (ret == 1);
            }
            return false;
        } finally {
            indexLock.unlock();
        }
    }

    public SearchCursor reader(@NonNull Query query, int pageSize, int pageBuffers) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        return new SearchCursor(baseDir, query)
                .withPageSize(pageSize)
                .withPageBuffers(pageBuffers)
                .create(executor);
    }

    public SearchCursor reader(@NonNull Query query) throws Exception {
        return reader(query, -1, -1);
    }

    public Document findById(@NonNull String id) throws Exception {
        return findBy(InodeIndexConstants.NAME_UUID, id);
    }

    public Document findByFsPath(@NonNull String path) throws Exception {
        return findBy(InodeIndexConstants.NAME_FS_PATH, path);
    }

    public Document findByZkPath(@NonNull String path) throws Exception {
        return findBy(InodeIndexConstants.NAME_ZK_PATH, path);
    }

    public SearchCursor findByDirectory(@NonNull String path) throws Exception {
        Query query = new PrefixQuery(new Term(InodeIndexConstants.NAME_FS_PATH, path));
        return reader(query);
    }

    public Document findBy(@NonNull String name, @NonNull String value) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        Query query = new TermQuery(new Term(name, String.format("\"%s\"", value)));
        TopDocs topDocs = search.search(query, 1);
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = search.doc(scoreDoc.doc);
            String v = doc.get(name);
            if (v != null) {
                if (v.compareTo(value) == 0) {
                    return doc;
                }
            }
        }
        return null;
    }

    public SearchCursor find(@NonNull FileIndexFilter filter) throws Exception {
        Preconditions.checkState(state != null && state.initialized());
        Query query = filter.build();
        return reader(query);
    }

    @Override
    public void close() throws IOException {
        if (writer != null && writer.isOpen()) {
            writer.flush();
            writer.close();
        }
        writer = null;
        if (search != null) {
            search = null;
        }
        state = null;
    }

    @Override
    public void visit(@NonNull Operation op,
                      @NonNull OperationState state,
                      @NonNull Inode inode,
                      Throwable error) {
        try {
            if (state == OperationState.Error) return;
            switch (op) {
                case Create, Update -> {
                    if (inode instanceof FileInode)
                        index((FileInode) inode);
                }
                case Delete -> checkAndDelete(inode);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void checkAndDelete(Inode node) throws Exception {
        if (node instanceof FileInode) {
            delete((FileInode) node);
        } else {
            try (SearchCursor cursor = findByDirectory(node.getFsPath())) {
                indexLock.lock();
                try {
                    state = getState();
                    while (true) {
                        Collection<Document> docs = cursor.fetch();
                        if (docs == null) break;
                        for (Document doc : docs) {
                            String fsPath = doc.get(InodeIndexConstants.NAME_FS_PATH);
                            if (!Strings.isNullOrEmpty(fsPath)) {
                                long ret = writer.deleteDocuments(
                                        new Term(InodeIndexConstants.NAME_FS_PATH, String.format("\"%s\"", fsPath)));
                                if (ret > 1) {
                                    throw new Exception(
                                            String.format("Index corrupted: multiple document deleted. [count=%s][faPath=%s]",
                                                    ret, fsPath));
                                }
                                DefaultLogger.trace(String.format("Delete index: [path=%s]", fsPath));
                                state.setCount(state().getCount() - 1);
                            }
                        }
                    }
                    state = save(state);
                } finally {
                    indexLock.unlock();
                }
            }
        }
    }
}
