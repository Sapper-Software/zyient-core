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

package io.zyient.base.core.utils.git;

import io.zyient.base.common.config.ConfigReader;
import io.zyient.base.common.utils.DefaultLogger;
import io.zyient.base.common.utils.PathUtils;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.transport.FetchResult;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Getter
@Accessors(fluent = true)
public class GitSyncer {
    public static final String __CONFIG_PATH = "repository";
    public static final String GIT_FILE_NAME = ".git";
    public static final String GIT_CONST_ORIGIN = "origin";

    private GitSyncerSettings settings;
    private File baseDir;
    private File gitfile;
    private Git handle;
    private Repository repository;

    public GitSyncer configure(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig) throws ConfigurationException {
        try {
            ConfigReader reader = new ConfigReader(xmlConfig, __CONFIG_PATH, GitSyncerSettings.class);
            reader.read();
            return configure((GitSyncerSettings) reader.settings());
        } catch (Exception ex) {
            DefaultLogger.stacktrace(ex);
            throw new ConfigurationException(ex);
        }
    }

    public GitSyncer configure(@NonNull GitSyncerSettings settings) throws ConfigurationException {
        try {
            this.settings = settings;
            baseDir = new File(settings.getBaseDir());
            if (!baseDir.exists()) {
                if (!baseDir.mkdirs()) {
                    throw new IOException(String.format("Failed to create local directory. [path=%s]",
                            baseDir.getAbsolutePath()));
                }
            }
            gitfile = new File(getFilePath(GIT_FILE_NAME));
            init(settings);
            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    private void init(GitSyncerSettings settings) throws Exception {
        if (!gitfile.exists()) {
            handle = Git.cloneRepository()
                    .setURI(settings.getUrl())
                    .setDirectory(baseDir)
                    .setBranchesToClone(List.of(settings.getBranch()))
                    .setBranch(settings.getBranch())
                    .call();
        } else {
            handle = Git.open(gitfile);
            if (settings.isSyncOnStart()) {
                FetchResult r = handle.fetch()
                        .setRemote(GIT_CONST_ORIGIN)
                        .call();
                if (DefaultLogger.isTraceEnabled()) {
                    DefaultLogger.trace(r);
                }
            }
        }
        repository = handle.getRepository();
    }


    private String getFilePath(String path) {
        return PathUtils.formatPath(String.format("%s/%s", baseDir.getAbsolutePath(), path));
    }
}
