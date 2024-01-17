/*
 * Copyright(C) (2024) Sapper Inc. (open.source at zyient dot io)
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

package io.zyient.base.core.decisions;

import com.google.common.base.Preconditions;
import io.zyient.base.common.model.ValidationException;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true)
public class OrCondition<T> implements Condition<T> {
    private Condition<T> left;
    private Condition<T> right;

    @Override
    public boolean evaluate(@NonNull T data) throws Exception {
        Preconditions.checkNotNull(left);
        Preconditions.checkNotNull(right);

        return (left.evaluate(data) || right.evaluate(data));
    }

    @Override
    public void validate() throws ValidationException {
        if (left == null) {
            throw new ValidationException("Missing left predicate...");
        }
        left.validate();
        if (right == null) {
            throw new ValidationException("Missing right predicate...");
        }
        right.validate();
    }
}
