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
import cz.jirutka.rsql.parser.ast.*;
import io.zyient.base.core.sources.email.MailDataStore;
import io.zyient.intake.utils.DateUtils;
import io.zyient.intake.utils.MailUtils;
import jakarta.mail.Flags;
import jakarta.mail.Message;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.search.*;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MessageQueryBuilder {
    public static final String TERM_FROM = "from";
    public static final String TERM_TO = "to";
    public static final String TERM_CC = "cc";
    public static final String TERM_BCC = "bcc";
    public static final String TERM_RECEIVED_DATE = "received";
    public static final String TERM_SENT_DATE = "sent";
    public static final String TERM_SUBJECT = "subject";
    public static final String TERM_BODY = "body";
    public static final String TERM_HEADER_PREFIX = "header#";
    public static final String TERM_FLAG_SEEN = "read";
    public static final String TERM_FLAG_ANSWERED = "replied";
    public static final String TERM_FLAG_DELETED = "deleted";
    public static final String TERM_FLAG_DRAFT = "draft";
    public static final String TERM_FLAG_RECENT = "recent";
    public static final String TERM_FLAG_FLAGGED = "flagged";

    public enum EOperator {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanEquals,
        LessThan,
        LessThanEquals,
        In,
        NotIn,
        Like,
        NotLike
    }

    private StringBuffer query;
    private String folder = MailDataStore.DEFAULT_IMAP_FOLDER;

    public MessageQueryBuilder build() {
        query = new StringBuffer();
        return this;
    }

    public String getQuery() {
        if (query != null) {
            return MailUtils.getQuery(folder, query.toString());
        }
        return null;
    }

    public MessageQueryBuilder folder(@Nonnull String folder) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(folder));
        this.folder = folder;
        return this;
    }

    public MessageQueryBuilder from(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_FROM, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder to(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_TO, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder to(@Nonnull EOperator operator, @Nonnull List<String> values) throws Exception {
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s", TERM_TO, oper));
        for (String value : values) {
            query.append(encode(value));
        }
        query.append(")");

        return this;
    }

    public MessageQueryBuilder cc(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_CC, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder cc(@Nonnull EOperator operator, @Nonnull List<String> values) throws Exception {
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s", TERM_CC, oper));
        for (String value : values) {
            query.append(encode(value));
        }
        query.append(")");

        return this;
    }

    public MessageQueryBuilder bcc(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_BCC, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder bcc(@Nonnull EOperator operator, @Nonnull List<String> values) throws Exception {
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s", TERM_BCC, oper));
        for (String value : values) {
            query.append(encode(value));
        }
        query.append(")");

        return this;
    }

    public MessageQueryBuilder sent(@Nonnull EOperator operator, @Nonnull Date date,
                                    String format) throws Exception {
        Preconditions.checkArgument(date != null);
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        if (Strings.isNullOrEmpty(format)) {
            format = DateUtils.DEFAULT_DATE_FORMAT;
        }
        SimpleDateFormat fmt = new SimpleDateFormat(format);
        String dates = fmt.format(date);
        query.append(String.format("%s%s%s", TERM_SENT_DATE, oper,
                encode(String.format("%s%s%s",
                        dates, DateUtils.FORMAT_PREFIX, format))));
        return this;
    }

    public MessageQueryBuilder sent(@Nonnull EOperator operator, long timestamp) throws Exception {
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);

        query.append(String.format("%s%s%d", TERM_SENT_DATE, oper, timestamp));
        return this;
    }

    public MessageQueryBuilder received(@Nonnull EOperator operator, @Nonnull Date date,
                                        String format) throws Exception {
        Preconditions.checkArgument(date != null);
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        if (Strings.isNullOrEmpty(format)) {
            format = DateUtils.DEFAULT_DATE_FORMAT;
        }
        SimpleDateFormat fmt = new SimpleDateFormat(format);
        String dates = fmt.format(date);
        query.append(String.format("%s%s%s", TERM_RECEIVED_DATE, oper,
                encode(String.format("%s%s%s",
                        dates, DateUtils.FORMAT_PREFIX, format))));
        return this;
    }

    public MessageQueryBuilder received(@Nonnull EOperator operator, long timestamp) throws Exception {
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);

        query.append(String.format("%s%s%d", TERM_RECEIVED_DATE, oper, timestamp));
        return this;
    }

    public MessageQueryBuilder flag(@Nonnull Flags.Flag flag, boolean value) {
        String flags = null;
        if (flag == Flags.Flag.ANSWERED) {
            flags = TERM_FLAG_ANSWERED;
        } else if (flag == Flags.Flag.DELETED) {
            flags = TERM_FLAG_DELETED;
        } else if (flag == Flags.Flag.DRAFT) {
            flags = TERM_FLAG_DRAFT;
        } else if (flag == Flags.Flag.FLAGGED) {
            flags = TERM_FLAG_FLAGGED;
        } else if (flag == Flags.Flag.RECENT) {
            flags = TERM_FLAG_RECENT;
        } else if (flag == Flags.Flag.SEEN) {
            flags = TERM_FLAG_SEEN;
        }
        query.append(String.format("%s==%s", flags, String.valueOf(value)));

        return this;
    }

    public MessageQueryBuilder header(@Nonnull String name, @Nonnull EOperator operator,
                                      @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        String oper = getOperator(operator);

        query.append(String.format("%s%s%s%s", TERM_HEADER_PREFIX, name, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder subject(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_SUBJECT, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder body(@Nonnull EOperator operator, @Nonnull String value) throws Exception {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(value));
        Preconditions.checkState(query != null);
        String oper = getOperator(operator);
        query.append(String.format("%s%s%s", TERM_BODY, oper, encode(value)));
        return this;
    }

    public MessageQueryBuilder or() {
        Preconditions.checkState(query != null);
        query.append(",");
        return this;
    }

    public MessageQueryBuilder and() {
        Preconditions.checkState(query != null);
        query.append(";");
        return this;
    }

    private String encode(String value) throws Exception {
        value = value.trim();
        if (!StringUtils.isNumeric(value) && !value.startsWith("(")) {
            value = String.format("'%s'", value);
        }
        return value;
    }

    private String decode(String value) throws Exception {
        return value;
    }

    public SearchTerm createSpecification(Node node) {
        if (node instanceof LogicalNode) {
            return createSpecification((LogicalNode) node);
        }
        if (node instanceof ComparisonNode) {
            return createSpecification((ComparisonNode) node);
        }
        return null;
    }

    public SearchTerm createSpecification(LogicalNode logicalNode) {
        List<SearchTerm> specs = logicalNode.getChildren()
                .stream()
                .map(node -> createSpecification(node))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        SearchTerm result = specs.get(0);
        if (logicalNode.getOperator() == LogicalOperator.AND) {
            for (int i = 1; i < specs.size(); i++) {
                result = new AndTerm(result, specs.get(i));
            }
        } else if (logicalNode.getOperator() == LogicalOperator.OR) {
            for (int i = 1; i < specs.size(); i++) {
                result = new OrTerm(result, specs.get(i));
            }
        }

        return result;
    }

    public SearchTerm createSpecification(ComparisonNode comparisonNode) {

        String field = comparisonNode.getSelector();
        EOperator oper = parseOperator(comparisonNode.getOperator());
        List<String> values = comparisonNode.getArguments();

        try {
            if (!Strings.isNullOrEmpty(field) && oper != null) {
                if (field.compareToIgnoreCase(TERM_FROM) == 0) {
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        if (!Strings.isNullOrEmpty(value)) {
                            FromTerm ft = new FromTerm(new InternetAddress(value));
                            if (oper == EOperator.Equals) {
                                return ft;
                            } else if (oper == EOperator.NotEquals) {
                                return new NotTerm(ft);
                            }
                        }
                    }
                } else if (field.compareToIgnoreCase(TERM_TO) == 0) {
                    return getRecipientTerm(Message.RecipientType.TO, values, oper);
                } else if (field.compareToIgnoreCase(TERM_CC) == 0) {
                    return getRecipientTerm(Message.RecipientType.CC, values, oper);
                } else if (field.compareToIgnoreCase(TERM_BCC) == 0) {
                    return getRecipientTerm(Message.RecipientType.BCC, values, oper);
                } else if (field.compareToIgnoreCase(TERM_RECEIVED_DATE) == 0) {
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        Date date = DateUtils.parse(value);
                        int dc = getDateCompare(oper);
                        return new ReceivedDateTerm(dc, date);
                    }
                } else if (field.compareToIgnoreCase(TERM_SENT_DATE) == 0) {
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        Date date = DateUtils.parse(value);
                        int dc = getDateCompare(oper);
                        return new SentDateTerm(dc, date);
                    }
                } else if (field.compareToIgnoreCase(TERM_FLAG_ANSWERED) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.ANSWERED), fv);
                } else if (field.compareToIgnoreCase(TERM_FLAG_DELETED) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.DELETED), fv);
                } else if (field.compareToIgnoreCase(TERM_FLAG_DRAFT) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.DRAFT), fv);
                } else if (field.compareToIgnoreCase(TERM_FLAG_FLAGGED) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.FLAGGED), fv);
                } else if (field.compareToIgnoreCase(TERM_FLAG_RECENT) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.RECENT), fv);
                } else if (field.compareToIgnoreCase(TERM_FLAG_SEEN) == 0) {
                    boolean fv = true;
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        fv = Boolean.parseBoolean(value);
                    }
                    return new FlagTerm(new Flags(Flags.Flag.SEEN), fv);
                } else if (field.compareToIgnoreCase(TERM_SUBJECT) == 0) {
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        SubjectTerm st = new SubjectTerm(value);
                        if (oper == EOperator.Like || oper == EOperator.Equals) {
                            return st;
                        } else if (oper == EOperator.NotLike || oper == EOperator.NotEquals) {
                            return new NotTerm(st);
                        }
                    }
                } else if (field.compareToIgnoreCase(TERM_BODY) == 0) {
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        BodyTerm bt = new BodyTerm(value);
                        if (oper == EOperator.Like || oper == EOperator.Equals) {
                            return bt;
                        } else if (oper == EOperator.NotLike || oper == EOperator.NotEquals) {
                            return new NotTerm(bt);
                        }
                    }
                } else if (field.startsWith(TERM_HEADER_PREFIX)) {
                    String term = field.replace(TERM_HEADER_PREFIX, "");
                    if (values != null && values.size() > 0) {
                        String value = decode(values.get(0));
                        return new HeaderTerm(term, value);
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        throw new RuntimeException(String.format("Error processing comparison node. [node=%s]",
                comparisonNode.toString()));
    }

    private int getDateCompare(EOperator oper) {
        int dt = DateTerm.EQ;
        switch (oper) {
            case Equals:
                dt = DateTerm.EQ;
                break;
            case NotEquals:
                dt = DateTerm.NE;
                break;
            case LessThan:
                dt = DateTerm.LT;
                break;
            case LessThanEquals:
                dt = DateTerm.LE;
                break;
            case GreaterThan:
                dt = DateTerm.GT;
                break;
            case GreaterThanEquals:
                dt = DateTerm.GE;
                break;
        }
        return dt;
    }

    private SearchTerm getRecipientTerm(Message.RecipientType type, List<String> values,
                                        EOperator oper) throws Exception {
        if (values != null && values.size() > 0) {
            if (values.size() == 1) {
                String value = decode(values.get(0));
                if (!Strings.isNullOrEmpty(value)) {
                    RecipientTerm rt = new RecipientTerm(type,
                            new InternetAddress(value));
                    if (oper == EOperator.Equals) {
                        return rt;
                    } else if (oper == EOperator.NotEquals) {
                        return new NotTerm(rt);
                    }
                }
            } else {
                if (oper == EOperator.In) {
                    List<SearchTerm> terms = new ArrayList<>(values.size());
                    for (int ii = 0; ii < values.size(); ii++) {
                        String value = decode(values.get(ii));
                        if (!Strings.isNullOrEmpty(value)) {
                            RecipientTerm rt = new RecipientTerm(type,
                                    new InternetAddress(value));
                            terms.add(rt);
                        }
                    }
                    if (terms.size() > 0) {
                        SearchTerm[] arr = new SearchTerm[terms.size()];
                        for (int ii = 0; ii < terms.size(); ii++) {
                            arr[ii] = terms.get(ii);
                        }
                        return new OrTerm(arr);
                    }
                } else if (oper == EOperator.NotIn) {
                    List<SearchTerm> terms = new ArrayList<>(values.size());
                    for (int ii = 0; ii < values.size(); ii++) {
                        String value = decode(values.get(ii));
                        if (!Strings.isNullOrEmpty(value)) {
                            RecipientTerm rt = new RecipientTerm(type,
                                    new InternetAddress(value));
                            terms.add(rt);
                        }
                    }
                    if (terms.size() > 0) {
                        SearchTerm[] arr = (SearchTerm[]) terms.toArray();
                        return new NotTerm(new AndTerm(arr));
                    }
                }
            }
        }
        return null;
    }

    private EOperator parseOperator(ComparisonOperator operator) {
        if (operator != null) {
            EOperator oper = null;
            if (operator == RSQLOperators.EQUAL) {
                oper = EOperator.Equals;
            } else if (operator == RSQLOperators.NOT_EQUAL) {
                oper = EOperator.NotEquals;
            } else if (operator == RSQLOperators.GREATER_THAN) {
                oper = EOperator.GreaterThan;
            } else if (operator == RSQLOperators.GREATER_THAN_OR_EQUAL) {
                oper = EOperator.GreaterThanEquals;
            } else if (operator == RSQLOperators.LESS_THAN) {
                oper = EOperator.LessThan;
            } else if (operator == RSQLOperators.LESS_THAN_OR_EQUAL) {
                oper = EOperator.LessThanEquals;
            } else if (operator == RSQLOperators.IN) {
                oper = EOperator.In;
            } else if (operator == RSQLOperators.NOT_IN) {
                oper = EOperator.NotIn;
            }
            return oper;
        }
        return null;
    }

    private String getOperator(@Nonnull EOperator operator) {
        switch (operator) {
            case GreaterThanEquals:
                return ">=";
            case GreaterThan:
                return ">";
            case LessThanEquals:
                return "<=";
            case LessThan:
                return "<";
            case NotEquals:
            case NotLike:
                return "!=";
            case Equals:
            case Like:
                return "==";
            case In:
                return "=in=";
            case NotIn:
                return "=out=";
        }
        return null;
    }
}
