/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.query;

import org.apache.lucene.queries.spans.SpanContainingQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.index.query.SpanQueryBuilder.SpanQueryBuilderUtil.checkNoBoost;

/**
 * Builder for {@link org.apache.lucene.queries.spans.SpanContainingQuery}.
 *
 * @opensearch.internal
 */
public class SpanContainingQueryBuilder extends AbstractQueryBuilder<SpanContainingQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_containing";

    private static final ParseField BIG_FIELD = new ParseField("big");
    private static final ParseField LITTLE_FIELD = new ParseField("little");

    private final SpanQueryBuilder big;
    private final SpanQueryBuilder little;

    /**
     * @param big the big clause, it must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public SpanContainingQueryBuilder(SpanQueryBuilder big, SpanQueryBuilder little) {
        if (big == null) {
            throw new IllegalArgumentException("inner clause [big] cannot be null.");
        }
        if (little == null) {
            throw new IllegalArgumentException("inner clause [little] cannot be null.");
        }
        this.little = little;
        this.big = big;
    }

    /**
     * Read from a stream.
     */
    public SpanContainingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        big = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        little = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(big);
        out.writeNamedWriteable(little);
    }

    /**
     * @return the big clause, it must enclose {@code little} for a match.
     */
    public SpanQueryBuilder bigQuery() {
        return this.big;
    }

    /**
     * @return the little clause, it must be contained within {@code big} for a match.
     */
    public SpanQueryBuilder littleQuery() {
        return this.little;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(BIG_FIELD.getPreferredName());
        big.toXContent(builder, params);
        builder.field(LITTLE_FIELD.getPreferredName());
        little.toXContent(builder, params);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static SpanContainingQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        SpanQueryBuilder big = null;
        SpanQueryBuilder little = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BIG_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (!(query instanceof SpanQueryBuilder spanQuery)) {
                        throw new ParsingException(parser.getTokenLocation(), "span_containing [big] must be of type span query");
                    }
                    big = spanQuery;
                    checkNoBoost(NAME, currentFieldName, parser, big);
                } else if (LITTLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (!(query instanceof SpanQueryBuilder spanQuery)) {
                        throw new ParsingException(parser.getTokenLocation(), "span_containing [little] must be of type span query");
                    }
                    little = spanQuery;
                    checkNoBoost(NAME, currentFieldName, parser, little);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[span_containing] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                boost = parser.floatValue();
            } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                queryName = parser.text();
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[span_containing] query does not support [" + currentFieldName + "]"
                );
            }
        }

        SpanContainingQueryBuilder query = new SpanContainingQueryBuilder(big, little);
        query.boost(boost).queryName(queryName);
        return query;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerBig = big.toQuery(context);
        assert innerBig instanceof SpanQuery;
        Query innerLittle = little.toQuery(context);
        assert innerLittle instanceof SpanQuery;
        return new SpanContainingQuery((SpanQuery) innerBig, (SpanQuery) innerLittle);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(big, little);
    }

    @Override
    protected boolean doEquals(SpanContainingQueryBuilder other) {
        return Objects.equals(big, other.big) && Objects.equals(little, other.little);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        visitor.getChildVisitor(BooleanClause.Occur.MUST).accept(big);
        visitor.getChildVisitor(BooleanClause.Occur.MUST).accept(little);
    }
}
