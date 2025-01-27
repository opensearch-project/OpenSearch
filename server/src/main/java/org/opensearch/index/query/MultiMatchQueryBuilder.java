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

import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.support.QueryParsers;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.index.search.MultiMatchQuery;
import org.opensearch.index.search.QueryParserHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Same as {@link MatchQueryBuilder} but supports multiple fields.
 *
 * @opensearch.internal
 */
public class MultiMatchQueryBuilder extends AbstractQueryBuilder<MultiMatchQueryBuilder> {

    private static final String CUTOFF_FREQUENCY_DEPRECATION_MSG = "you can omit this option, "
        + "the [multi_match] query can skip block of documents efficiently if the total number of hits is not tracked";

    public static final String NAME = "multi_match";

    public static final MultiMatchQueryBuilder.Type DEFAULT_TYPE = MultiMatchQueryBuilder.Type.BEST_FIELDS;
    public static final Operator DEFAULT_OPERATOR = Operator.OR;
    public static final int DEFAULT_PHRASE_SLOP = MatchQuery.DEFAULT_PHRASE_SLOP;
    public static final int DEFAULT_PREFIX_LENGTH = FuzzyQuery.defaultPrefixLength;
    public static final int DEFAULT_MAX_EXPANSIONS = FuzzyQuery.defaultMaxExpansions;
    public static final MatchQuery.ZeroTermsQuery DEFAULT_ZERO_TERMS_QUERY = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;
    public static final boolean DEFAULT_FUZZY_TRANSPOSITIONS = FuzzyQuery.defaultTranspositions;

    private static final ParseField SLOP_FIELD = new ParseField("slop");
    private static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");
    private static final ParseField LENIENT_FIELD = new ParseField("lenient");
    private static final ParseField CUTOFF_FREQUENCY_FIELD = new ParseField("cutoff_frequency").withAllDeprecated(
        CUTOFF_FREQUENCY_DEPRECATION_MSG
    );
    private static final ParseField TIE_BREAKER_FIELD = new ParseField("tie_breaker");
    private static final ParseField FUZZY_REWRITE_FIELD = new ParseField("fuzzy_rewrite");
    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");
    private static final ParseField OPERATOR_FIELD = new ParseField("operator");
    private static final ParseField MAX_EXPANSIONS_FIELD = new ParseField("max_expansions");
    private static final ParseField PREFIX_LENGTH_FIELD = new ParseField("prefix_length");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField GENERATE_SYNONYMS_PHRASE_QUERY = new ParseField("auto_generate_synonyms_phrase_query");
    private static final ParseField FUZZY_TRANSPOSITIONS_FIELD = new ParseField("fuzzy_transpositions");

    private final Object value;
    private final Map<String, Float> fieldsBoosts;
    private Type type = DEFAULT_TYPE;
    private Operator operator = DEFAULT_OPERATOR;
    private String analyzer;
    private int slop = DEFAULT_PHRASE_SLOP;
    private Fuzziness fuzziness;
    private int prefixLength = DEFAULT_PREFIX_LENGTH;
    private int maxExpansions = DEFAULT_MAX_EXPANSIONS;
    private String minimumShouldMatch;
    private String fuzzyRewrite = null;
    private Float tieBreaker;
    private Boolean lenient;
    private Float cutoffFrequency = null;
    private MatchQuery.ZeroTermsQuery zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;
    private boolean autoGenerateSynonymsPhraseQuery = true;
    private boolean fuzzyTranspositions = DEFAULT_FUZZY_TRANSPOSITIONS;

    /**
     * Type of the match
     *
     * @opensearch.internal
     */
    public enum Type implements Writeable {

        /**
         * Uses the best matching boolean field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        BEST_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("best_fields", "boolean")),

        /**
         * Uses the sum of the matching boolean fields to score the query
         */
        MOST_FIELDS(MatchQuery.Type.BOOLEAN, 1.0f, new ParseField("most_fields")),

        /**
         * Uses a blended DocumentFrequency to dynamically combine the queried
         * fields into a single field given the configured analysis is identical.
         * This type uses a tie-breaker to adjust the score based on remaining
         * matches per analyzed terms
         */
        CROSS_FIELDS(MatchQuery.Type.BOOLEAN, 0.0f, new ParseField("cross_fields")),

        /**
         * Uses the best matching phrase field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        PHRASE(MatchQuery.Type.PHRASE, 0.0f, new ParseField("phrase")),

        /**
         * Uses the best matching phrase-prefix field as main score and uses
         * a tie-breaker to adjust the score based on remaining field matches
         */
        PHRASE_PREFIX(MatchQuery.Type.PHRASE_PREFIX, 0.0f, new ParseField("phrase_prefix")),

        /**
         * Uses the sum of the matching boolean fields to score the query
         */
        BOOL_PREFIX(MatchQuery.Type.BOOLEAN_PREFIX, 1.0f, new ParseField("bool_prefix"));

        private MatchQuery.Type matchQueryType;
        private final float tieBreaker;
        private final ParseField parseField;

        Type(MatchQuery.Type matchQueryType, float tieBreaker, ParseField parseField) {
            this.matchQueryType = matchQueryType;
            this.tieBreaker = tieBreaker;
            this.parseField = parseField;
        }

        public float tieBreaker() {
            return this.tieBreaker;
        }

        public MatchQuery.Type matchQueryType() {
            return matchQueryType;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static Type parse(String value, DeprecationHandler deprecationHandler) {
            MultiMatchQueryBuilder.Type[] values = MultiMatchQueryBuilder.Type.values();
            Type type = null;
            for (MultiMatchQueryBuilder.Type t : values) {
                if (t.parseField().match(value, deprecationHandler)) {
                    type = t;
                    break;
                }
            }
            if (type == null) {
                throw new OpenSearchParseException("failed to parse [{}] query type [{}]. unknown type.", NAME, value);
            }
            return type;
        }

        public static Type readFromStream(StreamInput in) throws IOException {
            return Type.values()[in.readVInt()];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal());
        }
    }

    /**
     * Returns the type (for testing)
     */
    public MultiMatchQueryBuilder.Type getType() {
        return type;
    }

    /**
     * Constructs a new text query.
     */
    public MultiMatchQueryBuilder(Object value, String... fields) {
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        if (fields == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fields at initialization time");
        }
        this.value = value;
        this.fieldsBoosts = new TreeMap<>();
        for (String field : fields) {
            field(field);
        }
    }

    /**
     * Read from a stream.
     */
    public MultiMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
        value = in.readGenericValue();
        int size = in.readVInt();
        fieldsBoosts = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            float boost = in.readFloat();
            checkNegativeBoost(boost);
            fieldsBoosts.put(field, boost);
        }
        type = Type.readFromStream(in);
        operator = Operator.readFromStream(in);
        analyzer = in.readOptionalString();
        slop = in.readVInt();
        fuzziness = in.readOptionalWriteable(Fuzziness::new);
        prefixLength = in.readVInt();
        maxExpansions = in.readVInt();
        minimumShouldMatch = in.readOptionalString();
        fuzzyRewrite = in.readOptionalString();
        tieBreaker = in.readOptionalFloat();
        lenient = in.readOptionalBoolean();
        cutoffFrequency = in.readOptionalFloat();
        zeroTermsQuery = MatchQuery.ZeroTermsQuery.readFromStream(in);
        autoGenerateSynonymsPhraseQuery = in.readBoolean();
        fuzzyTranspositions = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeVInt(fieldsBoosts.size());
        for (Map.Entry<String, Float> fieldsEntry : fieldsBoosts.entrySet()) {
            out.writeString(fieldsEntry.getKey());
            out.writeFloat(fieldsEntry.getValue());
        }
        type.writeTo(out);
        operator.writeTo(out);
        out.writeOptionalString(analyzer);
        out.writeVInt(slop);
        out.writeOptionalWriteable(fuzziness);
        out.writeVInt(prefixLength);
        out.writeVInt(maxExpansions);
        out.writeOptionalString(minimumShouldMatch);
        out.writeOptionalString(fuzzyRewrite);
        out.writeOptionalFloat(tieBreaker);
        out.writeOptionalBoolean(lenient);
        out.writeOptionalFloat(cutoffFrequency);
        zeroTermsQuery.writeTo(out);
        out.writeBoolean(autoGenerateSynonymsPhraseQuery);
        out.writeBoolean(fuzzyTranspositions);
    }

    public Object value() {
        return value;
    }

    /**
     * Adds a field to run the multi match against.
     */
    public MultiMatchQueryBuilder field(String field) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        this.fieldsBoosts.put(field, AbstractQueryBuilder.DEFAULT_BOOST);
        return this;
    }

    /**
     * Adds a field to run the multi match against with a specific boost.
     */
    public MultiMatchQueryBuilder field(String field, float boost) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        checkNegativeBoost(boost);
        this.fieldsBoosts.put(field, boost);
        return this;
    }

    /**
     * Add several fields to run the query against with a specific boost.
     */
    public MultiMatchQueryBuilder fields(Map<String, Float> fields) {
        for (float fieldBoost : fields.values()) {
            checkNegativeBoost(fieldBoost);
        }
        this.fieldsBoosts.putAll(fields);
        return this;
    }

    public Map<String, Float> fields() {
        return fieldsBoosts;
    }

    /**
     * Sets the type of the text query.
     */
    public MultiMatchQueryBuilder type(MultiMatchQueryBuilder.Type type) {
        if (type == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires type to be non-null");
        }
        this.type = type;
        return this;
    }

    /**
     * Sets the type of the text query.
     */
    public MultiMatchQueryBuilder type(Object type) {
        if (type == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires type to be non-null");
        }
        this.type = Type.parse(type.toString().toLowerCase(Locale.ROOT), LoggingDeprecationHandler.INSTANCE);
        return this;
    }

    public Type type() {
        return type;
    }

    /**
     * Sets the operator to use when using a boolean query. Defaults to {@code OR}.
     */
    public MultiMatchQueryBuilder operator(Operator operator) {
        if (operator == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires operator to be non-null");
        }
        this.operator = operator;
        return this;
    }

    public Operator operator() {
        return operator;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public MultiMatchQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return analyzer;
    }

    /**
     * Set the phrase slop if evaluated to a phrase query type.
     */
    public MultiMatchQueryBuilder slop(int slop) {
        if (slop < 0) {
            throw new IllegalArgumentException("No negative slop allowed.");
        }
        this.slop = slop;
        return this;
    }

    public int slop() {
        return slop;
    }

    @Deprecated
    /*
      Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to "AUTO".
     */
    public MultiMatchQueryBuilder fuzziness(Object fuzziness) {
        if (fuzziness != null) {
            this.fuzziness = Fuzziness.build(fuzziness);
        }
        return this;
    }

    /**
     * Sets the fuzziness used when evaluated to a fuzzy query type. Defaults to "AUTO".
     */
    public MultiMatchQueryBuilder fuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
        return this;
    }

    public Fuzziness fuzziness() {
        return fuzziness;
    }

    public MultiMatchQueryBuilder prefixLength(int prefixLength) {
        if (prefixLength < 0) {
            throw new IllegalArgumentException("No negative prefix length allowed.");
        }
        this.prefixLength = prefixLength;
        return this;
    }

    public int prefixLength() {
        return prefixLength;
    }

    /**
     * When using fuzzy or prefix type query, the number of term expansions to use. Defaults to unbounded
     * so its recommended to set it to a reasonable value for faster execution.
     */
    public MultiMatchQueryBuilder maxExpansions(int maxExpansions) {
        if (maxExpansions <= 0) {
            throw new IllegalArgumentException("Max expansions must be strictly great than zero.");
        }
        this.maxExpansions = maxExpansions;
        return this;
    }

    public int maxExpansions() {
        return maxExpansions;
    }

    public MultiMatchQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    public MultiMatchQueryBuilder fuzzyRewrite(String fuzzyRewrite) {
        this.fuzzyRewrite = fuzzyRewrite;
        return this;
    }

    public String fuzzyRewrite() {
        return fuzzyRewrite;
    }

    /**
     * <p>Tie-Breaker for "best-match" disjunction queries (OR-Queries).
     * The tie breaker capability allows documents that match more than one query clause
     * (in this case on more than one field) to be scored better than documents that
     * match only the best of the fields, without confusing this with the better case of
     * two distinct matches in the multiple fields.</p>
     *
     * <p>A tie-breaker value of {@code 1.0} is interpreted as a signal to score queries as
     * "most-match" queries where all matching query clauses are considered for scoring.</p>
     *
     * @see Type
     */
    public MultiMatchQueryBuilder tieBreaker(float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    /**
     * <p>Tie-Breaker for "best-match" disjunction queries (OR-Queries).
     * The tie breaker capability allows documents that match more than one query clause
     * (in this case on more than one field) to be scored better than documents that
     * match only the best of the fields, without confusing this with the better case of
     * two distinct matches in the multiple fields.</p>
     *
     * <p>A tie-breaker value of {@code 1.0} is interpreted as a signal to score queries as
     * "most-match" queries where all matching query clauses are considered for scoring.</p>
     *
     * @see Type
     */
    public MultiMatchQueryBuilder tieBreaker(Float tieBreaker) {
        this.tieBreaker = tieBreaker;
        return this;
    }

    public Float tieBreaker() {
        return tieBreaker;
    }

    /**
     * Sets whether format based failures will be ignored.
     */
    public MultiMatchQueryBuilder lenient(boolean lenient) {
        this.lenient = lenient;
        return this;
    }

    public boolean lenient() {
        return lenient == null ? MatchQuery.DEFAULT_LENIENCY : lenient;
    }

    /**
     * Set a cutoff value in [0..1] (or absolute number &gt;=1) representing the
     * maximum threshold of a terms document frequency to be considered a low
     * frequency term.
     *
     * @deprecated Since max_score optimization landed in 7.0, normal MultiMatchQuery
     *             will achieve the same result without any configuration.
     */
    @Deprecated
    public MultiMatchQueryBuilder cutoffFrequency(float cutoff) {
        this.cutoffFrequency = cutoff;
        return this;
    }

    /**
     * Set a cutoff value in [0..1] (or absolute number &gt;=1) representing the
     * maximum threshold of a terms document frequency to be considered a low
     * frequency term.
     *
     * @deprecated Since max_score optimization landed in 7.0, normal MultiMatchQuery
     *             will achieve the same result without any configuration.
     */
    @Deprecated
    public MultiMatchQueryBuilder cutoffFrequency(Float cutoff) {
        this.cutoffFrequency = cutoff;
        return this;
    }

    public Float cutoffFrequency() {
        return cutoffFrequency;
    }

    public MultiMatchQueryBuilder zeroTermsQuery(MatchQuery.ZeroTermsQuery zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zero terms query to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public MatchQuery.ZeroTermsQuery zeroTermsQuery() {
        return zeroTermsQuery;
    }

    public MultiMatchQueryBuilder autoGenerateSynonymsPhraseQuery(boolean enable) {
        this.autoGenerateSynonymsPhraseQuery = enable;
        return this;
    }

    /**
     * Whether phrase queries should be automatically generated for multi terms synonyms.
     * Defaults to {@code true}.
     */
    public boolean autoGenerateSynonymsPhraseQuery() {
        return autoGenerateSynonymsPhraseQuery;
    }

    public boolean fuzzyTranspositions() {
        return fuzzyTranspositions;
    }

    /**
     * Sets whether transpositions are supported in fuzzy queries.<p>
     * The default metric used by fuzzy queries to determine a match is the Damerau-Levenshtein
     * distance formula which supports transpositions. Setting transposition to false will
     * switch to classic Levenshtein distance.<br>
     * If not set, Damerau-Levenshtein distance metric will be used.
     */
    public MultiMatchQueryBuilder fuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
        return this;
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), value);
        builder.startArray(FIELDS_FIELD.getPreferredName());
        for (Map.Entry<String, Float> fieldEntry : this.fieldsBoosts.entrySet()) {
            builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
        }
        builder.endArray();
        builder.field(TYPE_FIELD.getPreferredName(), type.toString().toLowerCase(Locale.ENGLISH));
        builder.field(OPERATOR_FIELD.getPreferredName(), operator.toString());
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(SLOP_FIELD.getPreferredName(), slop);
        if (fuzziness != null) {
            fuzziness.toXContent(builder, params);
        }
        builder.field(PREFIX_LENGTH_FIELD.getPreferredName(), prefixLength);
        builder.field(MAX_EXPANSIONS_FIELD.getPreferredName(), maxExpansions);
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        if (fuzzyRewrite != null) {
            builder.field(FUZZY_REWRITE_FIELD.getPreferredName(), fuzzyRewrite);
        }
        if (tieBreaker != null) {
            builder.field(TIE_BREAKER_FIELD.getPreferredName(), tieBreaker);
        }
        if (lenient != null) {
            builder.field(LENIENT_FIELD.getPreferredName(), lenient);
        }
        if (cutoffFrequency != null) {
            builder.field(CUTOFF_FREQUENCY_FIELD.getPreferredName(), cutoffFrequency);
        }
        builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        builder.field(GENERATE_SYNONYMS_PHRASE_QUERY.getPreferredName(), autoGenerateSynonymsPhraseQuery);
        builder.field(FUZZY_TRANSPOSITIONS_FIELD.getPreferredName(), fuzzyTranspositions);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static MultiMatchQueryBuilder fromXContent(XContentParser parser) throws IOException {
        Object value = null;
        Map<String, Float> fieldsBoosts = new HashMap<>();
        MultiMatchQueryBuilder.Type type = DEFAULT_TYPE;
        String analyzer = null;
        int slop = DEFAULT_PHRASE_SLOP;
        Fuzziness fuzziness = null;
        int prefixLength = DEFAULT_PREFIX_LENGTH;
        int maxExpansions = DEFAULT_MAX_EXPANSIONS;
        Operator operator = DEFAULT_OPERATOR;
        String minimumShouldMatch = null;
        String fuzzyRewrite = null;
        Boolean useDisMax = null;
        Float tieBreaker = null;
        Float cutoffFrequency = null;
        Boolean lenient = null;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;
        boolean autoGenerateSynonymsPhraseQuery = true;
        boolean fuzzyTranspositions = DEFAULT_FUZZY_TRANSPOSITIONS;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (FIELDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                if (token == XContentParser.Token.START_ARRAY) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        parseFieldAndBoost(parser, fieldsBoosts);
                    }
                } else if (token.isValue()) {
                    parseFieldAndBoost(parser, fieldsBoosts);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    value = parser.objectText();
                } else if (TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    type = MultiMatchQueryBuilder.Type.parse(parser.text(), parser.getDeprecationHandler());
                } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzer = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (SLOP_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    slop = parser.intValue();
                } else if (Fuzziness.FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzziness = Fuzziness.parse(parser);
                } else if (PREFIX_LENGTH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    prefixLength = parser.intValue();
                } else if (MAX_EXPANSIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    maxExpansions = parser.intValue();
                } else if (OPERATOR_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    operator = Operator.fromString(parser.text());
                } else if (MINIMUM_SHOULD_MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    minimumShouldMatch = parser.textOrNull();
                } else if (FUZZY_REWRITE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyRewrite = parser.textOrNull();
                } else if (TIE_BREAKER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    tieBreaker = parser.floatValue();
                } else if (CUTOFF_FREQUENCY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    cutoffFrequency = parser.floatValue();
                } else if (LENIENT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    lenient = parser.booleanValue();
                } else if (ZERO_TERMS_QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    String zeroTermsValue = parser.text();
                    if ("none".equalsIgnoreCase(zeroTermsValue)) {
                        zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
                    } else if ("all".equalsIgnoreCase(zeroTermsValue)) {
                        zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unsupported zero_terms_query value [" + zeroTermsValue + "]"
                        );
                    }
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (GENERATE_SYNONYMS_PHRASE_QUERY.match(currentFieldName, parser.getDeprecationHandler())) {
                    autoGenerateSynonymsPhraseQuery = parser.booleanValue();
                } else if (FUZZY_TRANSPOSITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    fuzzyTranspositions = parser.booleanValue();
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

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for multi_match query");
        }

        if (fuzziness != null && (type == Type.CROSS_FIELDS || type == Type.PHRASE || type == Type.PHRASE_PREFIX)) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Fuzziness not allowed for type [" + type.parseField.getPreferredName() + "]"
            );
        }

        if (slop != DEFAULT_PHRASE_SLOP && type == Type.BOOL_PREFIX) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + SLOP_FIELD.getPreferredName() + "] not allowed for type [" + type.parseField.getPreferredName() + "]"
            );
        }

        if (cutoffFrequency != null && type == Type.BOOL_PREFIX) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[" + CUTOFF_FREQUENCY_FIELD.getPreferredName() + "] not allowed for type [" + type.parseField.getPreferredName() + "]"
            );
        }

        MultiMatchQueryBuilder builder = new MultiMatchQueryBuilder(value).fields(fieldsBoosts)
            .type(type)
            .analyzer(analyzer)
            .cutoffFrequency(cutoffFrequency)
            .fuzziness(fuzziness)
            .fuzzyRewrite(fuzzyRewrite)
            .maxExpansions(maxExpansions)
            .minimumShouldMatch(minimumShouldMatch)
            .operator(operator)
            .prefixLength(prefixLength)
            .slop(slop)
            .tieBreaker(tieBreaker)
            .zeroTermsQuery(zeroTermsQuery)
            .autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery)
            .boost(boost)
            .queryName(queryName)
            .fuzzyTranspositions(fuzzyTranspositions);
        if (lenient != null) {
            builder.lenient(lenient);
        }
        return builder;
    }

    private static void parseFieldAndBoost(XContentParser parser, Map<String, Float> fieldsBoosts) throws IOException {
        String fField = null;
        Float fBoost = AbstractQueryBuilder.DEFAULT_BOOST;
        char[] fieldText = parser.textCharacters();
        int end = parser.textOffset() + parser.textLength();
        for (int i = parser.textOffset(); i < end; i++) {
            if (fieldText[i] == '^') {
                int relativeLocation = i - parser.textOffset();
                fField = new String(fieldText, parser.textOffset(), relativeLocation);
                fBoost = Float.parseFloat(new String(fieldText, i + 1, parser.textLength() - relativeLocation - 1));
                break;
            }
        }
        if (fField == null) {
            fField = parser.text();
        }
        fieldsBoosts.put(fField, fBoost);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MultiMatchQuery multiMatchQuery = new MultiMatchQuery(context);
        if (analyzer != null) {
            if (context.getIndexAnalyzers().get(analyzer) == null) {
                throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
            }
            multiMatchQuery.setAnalyzer(analyzer);
        }
        multiMatchQuery.setPhraseSlop(slop);
        if (fuzziness != null) {
            multiMatchQuery.setFuzziness(fuzziness);
        }
        multiMatchQuery.setFuzzyPrefixLength(prefixLength);
        multiMatchQuery.setMaxExpansions(maxExpansions);
        multiMatchQuery.setOccur(operator.toBooleanClauseOccur());
        if (fuzzyRewrite != null) {
            multiMatchQuery.setFuzzyRewriteMethod(
                QueryParsers.parseRewriteMethod(
                    fuzzyRewrite,
                    FuzzyQuery.defaultRewriteMethod(maxExpansions),
                    LoggingDeprecationHandler.INSTANCE
                )
            );
        }
        if (tieBreaker != null) {
            multiMatchQuery.setTieBreaker(tieBreaker);
        }
        if (cutoffFrequency != null) {
            multiMatchQuery.setCommonTermsCutoff(cutoffFrequency);
        }
        if (lenient != null) {
            multiMatchQuery.setLenient(lenient);
        }
        multiMatchQuery.setZeroTermsQuery(zeroTermsQuery);
        multiMatchQuery.setAutoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        multiMatchQuery.setTranspositions(fuzzyTranspositions);

        Map<String, Float> newFieldsBoosts;
        boolean isAllField;
        if (fieldsBoosts.isEmpty()) {
            // no fields provided, defaults to index.query.default_field
            List<String> defaultFields = context.defaultFields();
            newFieldsBoosts = QueryParserHelper.resolveMappingFields(context, QueryParserHelper.parseFieldsAndWeights(defaultFields));
            isAllField = QueryParserHelper.hasAllFieldsWildcard(defaultFields);
        } else {
            newFieldsBoosts = QueryParserHelper.resolveMappingFields(context, fieldsBoosts);
            isAllField = QueryParserHelper.hasAllFieldsWildcard(fieldsBoosts.keySet());
        }
        if (isAllField && lenient == null) {
            // Sets leniency to true if not explicitly
            // set in the request
            multiMatchQuery.setLenient(true);
        }
        return multiMatchQuery.parse(type, newFieldsBoosts, value, minimumShouldMatch);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(
            value,
            fieldsBoosts,
            type,
            operator,
            analyzer,
            slop,
            fuzziness,
            prefixLength,
            maxExpansions,
            minimumShouldMatch,
            fuzzyRewrite,
            tieBreaker,
            lenient,
            cutoffFrequency,
            zeroTermsQuery,
            autoGenerateSynonymsPhraseQuery,
            fuzzyTranspositions
        );
    }

    @Override
    protected boolean doEquals(MultiMatchQueryBuilder other) {
        return Objects.equals(value, other.value)
            && Objects.equals(fieldsBoosts, other.fieldsBoosts)
            && Objects.equals(type, other.type)
            && Objects.equals(operator, other.operator)
            && Objects.equals(analyzer, other.analyzer)
            && Objects.equals(slop, other.slop)
            && Objects.equals(fuzziness, other.fuzziness)
            && Objects.equals(prefixLength, other.prefixLength)
            && Objects.equals(maxExpansions, other.maxExpansions)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch)
            && Objects.equals(fuzzyRewrite, other.fuzzyRewrite)
            && Objects.equals(tieBreaker, other.tieBreaker)
            && Objects.equals(lenient, other.lenient)
            && Objects.equals(cutoffFrequency, other.cutoffFrequency)
            && Objects.equals(zeroTermsQuery, other.zeroTermsQuery)
            && Objects.equals(autoGenerateSynonymsPhraseQuery, other.autoGenerateSynonymsPhraseQuery)
            && Objects.equals(fuzzyTranspositions, other.fuzzyTranspositions);
    }
}
