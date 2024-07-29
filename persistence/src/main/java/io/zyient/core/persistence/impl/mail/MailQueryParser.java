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

package io.zyient.core.persistence.impl.mail;

import jakarta.mail.internet.ParseException;
import jakarta.mail.search.SearchTerm;
import lombok.Getter;
import lombok.experimental.Accessors;


/**
 * Abstract Class for defining Mail Search query parsers.
 */
@Getter
@Accessors(fluent = true)
public abstract class MailQueryParser {
    protected SearchTerm searchTerm;

    /**
     * Parse the String query and get the Search Term handle.
     *
     * @param query - Input Query String.
     * @return - Parsed Search Term
     * @throws ParseException
     */
    public abstract SearchTerm parse(String query) throws ParseException;
}
