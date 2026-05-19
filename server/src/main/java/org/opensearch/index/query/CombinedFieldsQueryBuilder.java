/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CombinedFieldQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.QueryBuilder;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.search.QueryParserHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Combined fields query that treats multiple fields as a
 * single stream and scores terms as if you had indexed them
 * as a single term in a single field.
 *
 * @opensearch.experimental
 */
public class CombinedFieldsQueryBuilder extends AbstractQueryBuilder<CombinedFieldsQueryBuilder> {

    public static final String NAME = "combined_fields";

    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField OPERATOR_FIELD = new ParseField("operator");
    private static final ParseField MINIMUM_SHOULD_MATCH_FIELD = new ParseField("minimum_should_match");

    private Object queryValue;
    private Map<String, Float> fieldToWeight;
    private Operator operator = Operator.OR;
    private String minimumShouldMatch;

    /**
     * Constructor for CombinedFieldsQueryBuilder.
     * @param value
     * @param fields
     */
    public CombinedFieldsQueryBuilder(Object value, String... fields) {
        if (value == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] requires query value", NAME));
        }
        if (fields == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "[%s] requires field list", NAME));
        }
        this.queryValue = value;
        this.fieldToWeight = QueryParserHelper.parseFieldsAndWeights(Arrays.asList(fields));
    }

    /*
     * Create a CombinedFieldsQueryBuilder from a StreamInput.
     * Used in conjuction with doWriteTo.
     * Wire format:
     * - base query properties (set byte size by AbstractQueryBuilder base class)
     * - query value object (variable size)
     * - numFields of fieldToWeight map (var int)
     * - repeated field name and weight pairs (string, float)
     * - operator (enum)
     * - minimumShouldMatch (string)
     * @param in
     * @throws IOException
     */
    public CombinedFieldsQueryBuilder(StreamInput in) throws IOException {
        // First, read in the base query properties (boost, queryName, etc.)
        super(in);

        this.queryValue = in.readGenericValue();

        int numFields = in.readVInt();
        this.fieldToWeight = new HashMap<>();
        for (int i = 0; i < numFields; i++) {
            String field = in.readString();
            float weight = in.readFloat();
            this.fieldToWeight.put(field, weight);
        }

        this.operator = Operator.readFromStream(in);

        this.minimumShouldMatch = in.readOptionalString();
    }

    /*
     * doWriteTo writes the query to a StreamOutput.
     * Used in conjuction with the constructor that takes a StreamInput.
     * Wire format:
     * - base query properties (set byte size by AbstractQueryBuilder base class)
     * - query value object (variable size)
     * - numFields of fieldToWeight map (var int)
     * - repeated field name and weight pairs (string, float)
     * - operator (enum)
     * - minimumShouldMatch (string)
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(queryValue);

        out.writeVInt(fieldToWeight.size());
        for (Map.Entry<String, Float> fieldEntry : fieldToWeight.entrySet()) {
            out.writeString(fieldEntry.getKey());
            out.writeFloat(fieldEntry.getValue());
        }

        this.operator.writeTo(out);

        out.writeOptionalString(minimumShouldMatch);
    }

    // ========================
    // FIELD ACCESSORS
    // ========================

    /**
     * Returns the query value to be analyzed and matched.
     */
    public Object queryValue() {
        return this.queryValue;
    }

    /**
     * Returns the map of field names to their boost values.
     */
    public Map<String, Float> fieldToWeight() {
        return this.fieldToWeight;
    }

    /**
     * Returns the operator used to combine terms.
     */
    public Operator operator() {
        return this.operator;
    }

    /**
     * Sets the operator used to combine terms (AND or OR).
     *
     * @param operator the operator to use
     * @return this query builder for method chaining
     */
    public CombinedFieldsQueryBuilder operator(Operator operator) {
        this.operator = operator;
        return this;
    }

    public CombinedFieldsQueryBuilder minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
        return this;
    }

    public String minimumShouldMatch() {
        return this.minimumShouldMatch;
    }

    // ========================
    // XCONTENT SERIALIZATION
    // ========================

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), queryValue);
        builder.startArray(FIELDS_FIELD.getPreferredName());
        for (Map.Entry<String, Float> fieldEntry : this.fieldToWeight.entrySet()) {
            builder.value(String.format(Locale.ROOT, "%s^%s", fieldEntry.getKey(), fieldEntry.getValue()));
        }
        builder.endArray();
        builder.field(OPERATOR_FIELD.getPreferredName(), operator.toString());
        if (minimumShouldMatch != null) {
            builder.field(MINIMUM_SHOULD_MATCH_FIELD.getPreferredName(), minimumShouldMatch);
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    // ========================
    // XContent PARSING
    // ========================

    private static final ConstructingObjectParser<CombinedFieldsQueryBuilder, Void> XCONTENT_PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new CombinedFieldsQueryBuilder(a[0])
    );

    static {
        XCONTENT_PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);

        XCONTENT_PARSER.declareStringArray((builder, values) -> {
            Map<String, Float> fieldToWeight = QueryParserHelper.parseFieldsAndWeights(values);
            builder.fieldToWeight = fieldToWeight;
        }, FIELDS_FIELD);

        XCONTENT_PARSER.declareString(CombinedFieldsQueryBuilder::operator, Operator::fromString, OPERATOR_FIELD);

        XCONTENT_PARSER.declareFloat(CombinedFieldsQueryBuilder::boost, BOOST_FIELD);
        XCONTENT_PARSER.declareString(CombinedFieldsQueryBuilder::queryName, NAME_FIELD);
        XCONTENT_PARSER.declareField(
            CombinedFieldsQueryBuilder::minimumShouldMatch,
            (p, c) -> p.textOrNull(),
            MINIMUM_SHOULD_MATCH_FIELD,
            ObjectParser.ValueType.VALUE
        );
    }

    public static CombinedFieldsQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return XCONTENT_PARSER.parse(parser, null);
    }

    // ========================
    // ABSTRACT METHOD IMPLEMENTATIONS
    // ========================

    @Override
    public String getWriteableName() {
        return NAME;
    }

    // ========================
    // CORE QUERY BUILDING LOGIC
    // ========================

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {

        Map<String, Float> mappedFields = QueryParserHelper.resolveMappingFields(context, fieldToWeight);

        if (mappedFields.isEmpty()) {
            return Queries.newUnmappedFieldsQuery(fieldToWeight.keySet());
        }

        List<MappedFieldType> fieldTypes = extractAndValidateMappedFieldTypes(context, mappedFields);
        Analyzer sharedAnalyzer = validateAndGetSharedAnalyzer(fieldTypes);

        return buildQuery(mappedFields, sharedAnalyzer);
    }

    // ========================
    // HELPER METHODS
    // ========================

    /**
     * Builds the list of mapped fields.
     */
    private List<MappedFieldType> extractAndValidateMappedFieldTypes(QueryShardContext context, Map<String, Float> mappedFields) {
        List<MappedFieldType> fields = new ArrayList<>();

        for (Map.Entry<String, Float> entry : mappedFields.entrySet()) {
            String name = entry.getKey();
            MappedFieldType fieldType = context.getFieldType(name);
            if (fieldType == null) {
                continue;
            }

            validateFieldType(fieldType);
            fields.add(fieldType);
        }

        return fields;
    }

    /**
     * Validates that the field type is supported by combined fields queries.
     */
    private void validateFieldType(MappedFieldType fieldType) {
        if (fieldType.familyTypeName().equals(TextFieldMapper.CONTENT_TYPE) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Field [%s] of type [%s] does not support [%s] queries",
                    fieldType.name(),
                    fieldType.typeName(),
                    NAME
                )
            );
        }
    }

    /**
     * Validates that all fields use the same analyzer and returns the shared analyzer.
     * This is a Lucene requirement for CombinedFieldQuery.
     */
    private Analyzer validateAndGetSharedAnalyzer(List<MappedFieldType> fieldTypes) {
        Analyzer sharedAnalyzer = null;

        for (MappedFieldType fieldType : fieldTypes) {
            Analyzer analyzer = fieldType.getTextSearchInfo().getSearchAnalyzer();
            if (sharedAnalyzer != null && analyzer.equals(sharedAnalyzer) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "All fields in [%s] query must have the same search analyzer", NAME)
                );
            }
            sharedAnalyzer = analyzer;
        }

        return sharedAnalyzer;
    }

    /**
     * Builds the final query using the prepared fields and analyzer.
     */
    private Query buildQuery(Map<String, Float> mappedFieldToWeight, Analyzer sharedAnalyzer) {
        assert mappedFieldToWeight.isEmpty() == false;

        String placeholderFieldName = mappedFieldToWeight.keySet().iterator().next();
        Builder builder = new Builder(mappedFieldToWeight, sharedAnalyzer, this.operator, this.minimumShouldMatch);
        Query query = builder.createBooleanQuery(placeholderFieldName, this.queryValue.toString(), this.operator.toBooleanClauseOccur());

        return query == null ? createZeroTermsQuery() : Queries.maybeApplyMinimumShouldMatch(query, this.minimumShouldMatch);
    }

    private Query createZeroTermsQuery() {
        return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
    }

    // ========================
    // INNER CLASS
    // ========================

    /**
     * Internal builder class that extends Lucene's QueryBuilder to create
     * combined field queries using the BM25F ranking algorithm.
     */
    private static class Builder extends QueryBuilder {
        private final Map<String, Float> mappedFieldToWeight;
        private final Operator operator;
        private final String minimumShouldMatch;

        Builder(Map<String, Float> mappedFieldToWeight, Analyzer analyzer, Operator operator, String minimumShouldMatch) {
            super(analyzer);
            this.mappedFieldToWeight = mappedFieldToWeight;
            this.operator = operator;
            this.minimumShouldMatch = minimumShouldMatch;
        }

        @Override
        public Query createPhraseQuery(String field, String queryText, int phraseSlop) {
            throw new IllegalArgumentException("[combined_fields] queries don't support phrases");
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            throw new IllegalArgumentException("[combined_fields] queries don't support phrases");
        }

        @Override
        protected Query newSynonymQuery(String fieldPlaceholder, TermAndBoost[] terms) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            BooleanClause.Occur occur = this.operator.toBooleanClauseOccur();

            for (TermAndBoost termAndBoost : terms) {
                BytesRef term = termAndBoost.term();

                CombinedFieldQuery.Builder combinedFieldBuilder = new CombinedFieldQuery.Builder(term);
                for (Map.Entry<String, Float> entry : mappedFieldToWeight.entrySet()) {
                    combinedFieldBuilder.addField(entry.getKey(), entry.getValue());
                }

                builder.add(combinedFieldBuilder.build(), occur);
            }

            return builder.build();
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            QueryBuilder.TermAndBoost termAndBoost = new QueryBuilder.TermAndBoost(term.bytes(), boost);
            return newSynonymQuery("", new QueryBuilder.TermAndBoost[] { termAndBoost });
        }
    }

    // ========================
    // STANDARD QUERY BUILDER METHODS
    // ========================

    @Override
    protected int doHashCode() {
        return Objects.hash(queryValue, fieldToWeight, operator, minimumShouldMatch);
    }

    @Override
    protected boolean doEquals(CombinedFieldsQueryBuilder other) {
        return Objects.equals(queryValue, other.queryValue)
            && Objects.equals(fieldToWeight, other.fieldToWeight)
            && Objects.equals(operator, other.operator)
            && Objects.equals(minimumShouldMatch, other.minimumShouldMatch);
    }

}
