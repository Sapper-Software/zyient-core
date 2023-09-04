/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.io.indexing;

import com.google.common.base.Strings;
import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.core.BaseEnv;
import io.zyient.base.core.index.IndexBuilder;
import io.zyient.base.core.index.SearchCursor;
import io.zyient.base.core.io.FileSystem;
import io.zyient.base.core.io.impl.PostOperationVisitor;
import io.zyient.base.core.io.model.FileInode;
import io.zyient.base.core.io.model.Inode;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
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

    public void init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                     @NonNull BaseEnv<?> env,
                     @NonNull FileSystem fs) throws ConfigurationException {
        this.env = env;
        this.fs = fs;
        ConfigReader reader = new ConfigReader(config, FileSystemIndexerSettings.class);
        reader.read();
        settings = (FileSystemIndexerSettings) reader.settings();
        try {
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

            search = new IndexSearcher(DirectoryReader.open(baseDir), executor);
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void index(@NonNull FileInode inode) throws Exception {
        Document doc = builder.build(inode);
        writer.addDocument(doc);
    }

    public boolean delete(@NonNull FileInode inode) throws Exception {
        Document doc = findById(inode.getUuid());
        if (doc != null) {
            long ret = writer.deleteDocuments(
                    new Term(InodeIndexConstants.NAME_UUID, String.format("\"%s\"", inode.getUuid())));
            if (ret > 1) {
                throw new Exception(
                        String.format("Index corrupted: multiple document deleted. [count=%s][uuid=%s]",
                                ret, inode.getUuid()));
            }
            return (ret == 1);
        }
        return false;
    }

    public boolean delete(@NonNull String fsPath) throws Exception {
        Document doc = findByFsPath(fsPath);
        if (doc != null) {
            long ret = writer.deleteDocuments(
                    new Term(InodeIndexConstants.NAME_FS_PATH, String.format("\"%s\"", fsPath)));
            if (ret > 1) {
                throw new Exception(
                        String.format("Index corrupted: multiple document deleted. [count=%s][faPath=%s]",
                                ret, fsPath));
            }
            return (ret == 1);
        }
        return false;
    }

    public SearchCursor reader(@NonNull Query query, int pageSize, int pageBuffers) throws Exception {
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
    }

    @Override
    public void visit(@NonNull Operation op, @NonNull Inode inode) throws IOException {
        try {
            switch (op) {
                case Create, Update -> {
                    if (inode instanceof FileInode)
                        index((FileInode) inode);
                }
                case Delete -> checkAndDelete(inode);
            }
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    private void checkAndDelete(Inode node) throws Exception {
        if (node instanceof FileInode) {
            delete((FileInode) node);
        } else {
            try (SearchCursor cursor = findByDirectory(node.getFsPath())) {
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
                        }
                    }
                }
            }
        }
    }
}
