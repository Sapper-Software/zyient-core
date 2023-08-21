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

package ai.sapper.cdc.core.io.indexing;

import ai.sapper.cdc.common.config.ConfigReader;
import ai.sapper.cdc.core.BaseEnv;
import ai.sapper.cdc.core.index.IndexBuilder;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.model.FileInode;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

@Getter
@Accessors(fluent = true)
public class FileSystemIndexer implements Closeable {
    private FileSystemIndexerSettings settings;
    private FileSystem fs;
    private BaseEnv<?> env;
    private final Analyzer analyzer = new StandardAnalyzer();
    private final IndexBuilder builder = new IndexBuilder();
    private IndexWriter writer;
    private IndexSearcher search;

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
            Directory d = null;
            if (settings().isUseMappedFiles()) {
                d = new MMapDirectory(dir.toPath());
            } else {
                d = new NIOFSDirectory(dir.toPath());
            }
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            writer = new IndexWriter(d, indexWriterConfig);

            search = new IndexSearcher(DirectoryReader.open(d));
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public void index(@NonNull FileInode inode) throws Exception {
        Document doc = builder.build(inode);
        writer.addDocument(doc);
    }

    public void delete(@NonNull FileInode inode) throws Exception {

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
}
