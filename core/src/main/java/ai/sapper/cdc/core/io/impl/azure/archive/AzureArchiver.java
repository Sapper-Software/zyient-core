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

package ai.sapper.cdc.core.io.impl.azure.archive;

import ai.sapper.cdc.core.io.Archiver;
import ai.sapper.cdc.core.io.FileSystem;
import ai.sapper.cdc.core.io.impl.azure.AzureFsClient;
import ai.sapper.cdc.core.io.model.ArchivePathInfo;
import ai.sapper.cdc.core.io.model.ArchiverSettings;
import ai.sapper.cdc.core.io.model.PathInfo;
import lombok.NonNull;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.io.File;
import java.io.IOException;

public class AzureArchiver extends Archiver {
    private AzureFsClient client;

    @Override
    public Archiver init(@NonNull HierarchicalConfiguration<ImmutableNode> config,
                         String pathPrefix) throws IOException {
        try {
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Archiver init(@NonNull ArchiverSettings settings) throws IOException {
        try {
            return this;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public ArchivePathInfo archive(@NonNull PathInfo source,
                                   @NonNull ArchivePathInfo target,
                                   @NonNull FileSystem sourceFS) throws IOException {
        return null;
    }

    @Override
    public File getFromArchive(@NonNull String domain,
                               @NonNull String path) throws IOException {
        return null;
    }


    @Override
    public void close() throws IOException {

    }
}
