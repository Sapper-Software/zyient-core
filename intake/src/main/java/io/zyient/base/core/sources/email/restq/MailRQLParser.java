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

package io.zyient.base.core.sources.email.restq;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
import io.zyient.intake.utils.MailUtils;
import io.zyient.base.core.sources.email.MailQueryParser;
import jakarta.mail.internet.ParseException;
import jakarta.mail.search.SearchTerm;

import javax.annotation.Nonnull;

public class MailRQLParser extends MailQueryParser {

    /**
     * Parse the String query and get the Search Term handle.
     *
     * @param queryStr - Input Query String.
     * @return - Parsed Search Term
     * @throws ParseException
     */
    @Override
    public SearchTerm parse(@Nonnull String queryStr) throws ParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(queryStr));
        String query = MailUtils.getMailQuery(queryStr);
        if (Strings.isNullOrEmpty(query)) {
            throw new ParseException(String.format("Error extracting query from string. [string=%s]", queryStr));
        }
        Node rootNode = new RSQLParser().parse(query);
        MessageQueryVisitor visitor = new MessageQueryVisitor();
        searchTerm = rootNode.accept(visitor);
        return searchTerm;
    }

}
