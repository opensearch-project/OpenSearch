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

import org.apache.lucene.search.Query;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.index.search.MatchQuery.ZeroTermsQuery;

import java.io.IOException;
import java.util.Objects;

/**
 * Match query is a query that analyzes the text and constructs a phrase query
 * as the result of the analysis.
 *
 * @opensearch.internal
 */
public class MatchPhraseQueryBuilder extends AbstractQueryBuilder<MatchPhraseQueryBuilder> implements WithFieldName {
    public static final String NAME = "match_phrase";
    public static final ParseField SLOP_FIELD = new ParseField("slop");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private final String fieldName;

    private final Object value;

    private String analyzer;

    private int slop = MatchQuery.DEFAULT_PHRASE_SLOP;

    private ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;

    public MatchPhraseQueryBuilder(String fieldName, Object value) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public MatchPhraseQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        slop = in.readVInt();
        zeroTermsQuery = ZeroTermsQuery.readFromStream(in);
        analyzer = in.readOptionalString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        out.writeVInt(slop);
        zeroTermsQuery.writeTo(out);
        out.writeOptionalString(analyzer);
    }

    /** Returns the field name used in this query. */
    @Override
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping
     * config for the field, or, if not set, the default search analyzer.
     */
    public MatchPhraseQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Get the analyzer to use, if previously set, otherwise {@code null} */
    public String analyzer() {
        return this.analyzer;
    }

    /** Sets a slop factor for phrase queries */
    public MatchPhraseQueryBuilder slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("No negative slop allowed.");
        }
        this.slop = slop;
        return this;
    }

    /** Get the slop factor for phrase queries. */
    public int slop() {
        return this.slop;
    }

    /**
     * Sets query to use in case no query terms are available, e.g. after analysis removed them.
     * Defaults to {@link ZeroTermsQuery#NONE}, but can be set to
     * {@link ZeroTermsQuery#ALL} instead.
     */
    public MatchPhraseQueryBuilder zeroTermsQuery(ZeroTermsQuery zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zeroTermsQuery to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public ZeroTermsQuery zeroTermsQuery() {
        return this.zeroTermsQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);

        builder.field(MatchQueryBuilder.QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(MatchQueryBuilder.ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(SLOP_FIELD.getPreferredName(), slop);
        builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // validate context specific fields
        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
        }

        MatchQuery matchQuery = new MatchQuery(context);
        if (analyzer != null) {
            matchQuery.setAnalyzer(analyzer);
        }
        matchQuery.setPhraseSlop(slop);
        matchQuery.setZeroTermsQuery(zeroTermsQuery);

        return matchQuery.parse(MatchQuery.Type.PHRASE, fieldName, value);
    }

    @Override
    protected boolean doEquals(MatchPhraseQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(value, other.value)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(slop, other.slop)
            && Objects.equals(zeroTermsQuery, other.zeroTermsQuery);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, analyzer, slop);
    }

    public static MatchPhraseQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
        ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (MatchQueryBuilder.QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.objectText();
                        } else if (MatchQueryBuilder.ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            analyzer = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (SLOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            slop = parser.intValue();
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else if (ZERO_TERMS_QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            String zeroTermsValue = parser.text();
                            if ("none".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = ZeroTermsQuery.NONE;
                            } else if ("all".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = ZeroTermsQuery.ALL;
                            } else {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Unsupported zero_terms_query value [" + zeroTermsValue + "]"
                                );
                            }
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[" + NAME + "] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]"
                        );
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }

        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder(fieldName, value);
        matchQuery.analyzer(analyzer);
        matchQuery.slop(slop);
        matchQuery.zeroTermsQuery(zeroTermsQuery);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);
        return matchQuery;
    }
}
