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

package io.zyient.core.persistence.impl.mail.restq;

import cz.jirutka.rsql.parser.ast.AndNode;
import cz.jirutka.rsql.parser.ast.ComparisonNode;
import cz.jirutka.rsql.parser.ast.OrNode;
import cz.jirutka.rsql.parser.ast.RSQLVisitor;
import jakarta.mail.search.SearchTerm;
import lombok.Getter;


@Getter
public class MessageQueryVisitor implements RSQLVisitor<SearchTerm, Void> {
    private MessageQueryBuilder builder = new MessageQueryBuilder();

    @Override
    public SearchTerm visit(AndNode node, Void param) {
        return builder.createSpecification(node);
    }

    @Override
    public SearchTerm visit(OrNode node, Void param) {
        return builder.createSpecification(node);
    }

    @Override
    public SearchTerm visit(ComparisonNode node, Void param) {
        return builder.createSpecification(node);
    }
}
