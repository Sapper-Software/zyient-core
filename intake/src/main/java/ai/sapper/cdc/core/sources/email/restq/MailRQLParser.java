package ai.sapper.cdc.core.sources.email.restq;

import ai.sapper.cdc.core.sources.email.MailQueryParser;
import ai.sapper.cdc.intake.utils.MailUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import cz.jirutka.rsql.parser.RSQLParser;
import cz.jirutka.rsql.parser.ast.Node;
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
