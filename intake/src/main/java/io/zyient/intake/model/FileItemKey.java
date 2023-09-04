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

package io.zyient.intake.model;

import io.zyient.base.common.model.entity.IKey;
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
