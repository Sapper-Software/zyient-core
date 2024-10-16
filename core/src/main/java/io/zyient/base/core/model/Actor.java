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

package io.zyient.base.core.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.persistence.Embeddable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.http.auth.BasicUserPrincipal;

import java.security.Principal;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
@Embeddable
public class Actor extends UserOrRole {
    private long timestamp;

    public Actor() {
    }

    public Actor(@NonNull String name, @NonNull EUserOrRole type) {
        setName(name);
        setType(type);
        timestamp = System.currentTimeMillis();
    }

    public Actor(@NonNull UserOrRole user) {
        setName(user.getName());
        setType(user.getType());
        timestamp = System.currentTimeMillis();
    }

    @Override
    public Principal asPrincipal() throws Exception {
        return new BasicUserPrincipal(String.format("%s:[%s]", getType().name(), getName()));
    }
}
