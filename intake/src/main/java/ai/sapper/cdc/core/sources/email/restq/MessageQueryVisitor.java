package ai.sapper.cdc.core.sources.email.restq;

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
