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

import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;

/**
 * Query builder for field masking span query
 *
 * @opensearch.internal
 */
public class FieldMaskingSpanQueryBuilder extends AbstractQueryBuilder<FieldMaskingSpanQueryBuilder>
    implements
        SpanQueryBuilder,
        WithFieldName {

    public static final String NAME = "span_field_masking";
    public static final ParseField SPAN_FIELD_MASKING_FIELD = new ParseField(NAME, "field_masking_span");

    private static final ParseField FIELD_FIELD = new ParseField("field");
    private static final ParseField QUERY_FIELD = new ParseField("query");

    private final SpanQueryBuilder queryBuilder;

    private final String fieldName;

    /**
     * Constructs a new {@link FieldMaskingSpanQueryBuilder} given an inner {@link SpanQueryBuilder} for
     * a given field
     * @param queryBuilder inner {@link SpanQueryBuilder}
     * @param fieldName the field name
     */
    public FieldMaskingSpanQueryBuilder(SpanQueryBuilder queryBuilder, String fieldName) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("field name is null or empty");
        }
        if (queryBuilder == null) {
            throw new IllegalArgumentException("inner clause [query] cannot be null.");
        }
        this.queryBuilder = queryBuilder;
        this.fieldName = fieldName;
    }

    /**
     * Read from a stream.
     */
    public FieldMaskingSpanQueryBuilder(StreamInput in) throws IOException {
        super(in);
        queryBuilder = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        fieldName = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(queryBuilder);
        out.writeString(fieldName);
    }

    /**
     * @return the field name for this query
     */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /**
     * @return the inner {@link QueryBuilder}
     */
    public SpanQueryBuilder innerQuery() {
        return this.queryBuilder;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SPAN_FIELD_MASKING_FIELD.getPreferredName());
        builder.field(QUERY_FIELD.getPreferredName());
        queryBuilder.toXContent(builder, params);
        builder.field(FIELD_FIELD.getPreferredName(), fieldName);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static FieldMaskingSpanQueryBuilder fromXContent(XContentParser parser) throws IOException {
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder inner = null;
        String field = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + SPAN_FIELD_MASKING_FIELD.getPreferredName() + "] query must be of type span query"
                        );
                    }
                    inner = (SpanQueryBuilder) query;
                    SpanQueryBuilderUtil.checkNoBoost(SPAN_FIELD_MASKING_FIELD.getPreferredName(), currentFieldName, parser, inner);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + SPAN_FIELD_MASKING_FIELD.getPreferredName() + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (FIELD_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    field = parser.text();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + SPAN_FIELD_MASKING_FIELD.getPreferredName() + "] query does not support [" + currentFieldName + "]"
                    );
                }
            }
        }
        if (inner == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                SPAN_FIELD_MASKING_FIELD.getPreferredName() + " must have [query] span query clause"
            );
        }
        if (field == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                SPAN_FIELD_MASKING_FIELD.getPreferredName() + " must have [field] set for it"
            );
        }

        FieldMaskingSpanQueryBuilder queryBuilder = new FieldMaskingSpanQueryBuilder(inner, field);
        queryBuilder.boost(boost);
        queryBuilder.queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        String fieldInQuery = fieldName;
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType != null) {
            fieldInQuery = fieldType.name();
        }
        Query innerQuery = queryBuilder.toQuery(context);
        assert innerQuery instanceof SpanQuery;
        return new FieldMaskingSpanQuery((SpanQuery) innerQuery, fieldInQuery);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(queryBuilder, fieldName);
    }

    @Override
    protected boolean doEquals(FieldMaskingSpanQueryBuilder other) {
        return Objects.equals(queryBuilder, other.queryBuilder) && Objects.equals(fieldName, other.fieldName);
    }

    @Override
    public String getWriteableName() {
        return SPAN_FIELD_MASKING_FIELD.getPreferredName();
    }

    @Override
    public void visit(QueryBuilderVisitor visitor) {
        visitor.accept(this);
        visitor.getChildVisitor(BooleanClause.Occur.MUST).accept(queryBuilder);
    }
}
