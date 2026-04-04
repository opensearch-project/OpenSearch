/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.search.NotExistsQuery;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * A query that matches documents in which a given field does <em>not</em> exist (has no value).
 * <p>
 * This is the complement of {@link ExistsQueryBuilder}. It is semantically equivalent to:
 * <pre>
 * { "bool": { "must_not": [{ "exists": { "field": "fieldName" } }] } }
 * </pre>
 * but is expressed as a positive filter, making it eligible for Lucene's query cache.
 * <p>
 * Typical use: rewriting a {@code must_not: exists} clause on a dense field, where the
 * negation of a dense exists query would otherwise scan nearly the entire index.
 *
 * @opensearch.internal
 */
public class NotExistsQueryBuilder extends AbstractQueryBuilder<NotExistsQueryBuilder> implements WithFieldName {

    public static final String NAME = "not_exists";
    public static final ParseField FIELD_FIELD = new ParseField("field");

    private final String fieldName;

    public NotExistsQueryBuilder(String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public NotExistsQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryShardContext context = queryRewriteContext.convertToShardContext();
        if (context != null) {
            // If the field is not mapped, every document is effectively "missing" the field.
            Query innerExists = ExistsQueryBuilder.newFilter(context, fieldName, false);
            if (innerExists instanceof MatchNoDocsQuery) {
                return new MatchAllQueryBuilder();
            }
        }
        return super.doRewrite(queryRewriteContext);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerExists = ExistsQueryBuilder.newFilter(context, fieldName, false);

        if (innerExists instanceof MatchNoDocsQuery) {
            // Field is unmapped or absent everywhere → all docs match "not exists".
            return new MatchAllDocsQuery();
        }

        return new NotExistsQuery(innerExists, fieldName);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static NotExistsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldPattern = null;
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fieldPattern = parser.text();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NotExistsQueryBuilder.NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NotExistsQueryBuilder.NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                );
            }
        }

        if (fieldPattern == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + NotExistsQueryBuilder.NAME + "] must be provided with a [field]"
            );
        }

        NotExistsQueryBuilder builder = new NotExistsQueryBuilder(fieldPattern);
        builder.queryName(queryName);
        builder.boost(boost);
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName);
    }

    @Override
    protected boolean doEquals(NotExistsQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
