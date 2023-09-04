package ai.sapper.cdc.core.sources.email;

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
