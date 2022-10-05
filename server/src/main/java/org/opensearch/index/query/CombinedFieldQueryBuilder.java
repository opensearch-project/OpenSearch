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
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.ParseField;
import org.opensearch.common.ParsingException;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.search.MatchQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.opensearch.index.query.MultiMatchQueryBuilder.parseFieldAndBoost;

/**
 * Combined field query is a pure disjunction of query terms across the given fields,
 * where scoring pretends that the terms were all indexed to a single field.
 *
 * @see CombinedFieldQuery
 *
 * @opensearch.internal
 */
public class CombinedFieldQueryBuilder extends AbstractQueryBuilder<CombinedFieldQueryBuilder> {
    public static final String NAME = "combined_field";

    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField QUERY_FIELD = new ParseField("query");
    private static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    private static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private final Object value;
    private final Map<String, Float> fieldWeights;
    private String analyzer;
    private MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;

    public CombinedFieldQueryBuilder(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.value = value;
        this.fieldWeights = new TreeMap<>();
    }

    public CombinedFieldQueryBuilder(StreamInput in) throws IOException {
        super(in);
        value = in.readGenericValue();
        int size = in.readVInt();
        fieldWeights = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            float weight = in.readFloat();
            checkValidWeight(weight);
            fieldWeights.put(field, weight);
        }
        analyzer = in.readOptionalString();
        zeroTermsQuery = MatchQuery.ZeroTermsQuery.readFromStream(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeGenericValue(value);
        out.writeVInt(fieldWeights.size());
        for (Map.Entry<String, Float> fieldsEntry : fieldWeights.entrySet()) {
            out.writeString(fieldsEntry.getKey());
            out.writeFloat(fieldsEntry.getValue());
        }
        out.writeOptionalString(analyzer);
        zeroTermsQuery.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), value);
        builder.startArray(FIELDS_FIELD.getPreferredName());
        for (Map.Entry<String, Float> fieldEntry : fieldWeights.entrySet()) {
            builder.value(fieldEntry.getKey() + "^" + fieldEntry.getValue());
        }
        builder.endArray();
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static CombinedFieldQueryBuilder fromXContent(XContentParser parser) throws IOException {
        Object value = null;
        Map<String, Float> fieldWeights = new HashMap<>();
        String analyzer = null;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;

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
                        parseFieldAndBoost(parser, fieldWeights);
                    }
                } else if (token.isValue()) {
                    parseFieldAndBoost(parser, fieldWeights);
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support this value for [" + currentFieldName + "]"
                    );
                }
            } else if (token.isValue()) {
                if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    value = parser.objectText();
                } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    analyzer = parser.text();
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
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[" + NAME + "] query does not support [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[" + NAME + "] query does not support this value for [" + currentFieldName + "]"
                );
            }
        }

        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for " + NAME + " query");
        }

        return new CombinedFieldQueryBuilder(value).fields(fieldWeights)
            .analyzer(analyzer)
            .zeroTermsQuery(zeroTermsQuery)
            .queryName(queryName)
            .boost(boost);
    }

    public Object value() {
        return value;
    }

    /**
     * Adds a field to match against.
     */
    public CombinedFieldQueryBuilder field(String field) {
        return field(field, AbstractQueryBuilder.DEFAULT_BOOST);
    }

    /**
     * Adds a field to match against with a specific weight.
     */
    public CombinedFieldQueryBuilder field(String field, float weight) {
        if (Strings.isEmpty(field)) {
            throw new IllegalArgumentException("supplied field is null or empty.");
        }
        checkValidWeight(weight);
        this.fieldWeights.put(field, weight);
        return this;
    }

    private static void checkValidWeight(float weight) {
        if (Float.compare(weight, 1f) < 0) {
            throw new IllegalArgumentException(("weights must be greater or equal to 1 in [" + NAME + "]"));
        }
    }

    /**
     * Add several fields to run the query against with a specific weight.
     */
    public CombinedFieldQueryBuilder fields(Map<String, Float> fields) {
        for (Map.Entry<String, Float> fieldWeight : fields.entrySet()) {
            field(fieldWeight.getKey(), fieldWeight.getValue());
        }
        return this;
    }

    public Map<String, Float> fields() {
        return fieldWeights;
    }

    /**
     * Explicitly set the analyzer to use. Defaults to use explicit mapping config for the field, or, if not
     * set, the default search analyzer.
     */
    public CombinedFieldQueryBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public String analyzer() {
        return analyzer;
    }

    public CombinedFieldQueryBuilder zeroTermsQuery(MatchQuery.ZeroTermsQuery zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zero terms query to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    public MatchQuery.ZeroTermsQuery zeroTermsQuery() {
        return zeroTermsQuery;
    }

    static boolean isTextField(MappedFieldType fieldType) {
        if (fieldType == null) {
            return false;
        }
        return fieldType.getTextSearchInfo() != TextSearchInfo.NONE && fieldType.getTextSearchInfo() != TextSearchInfo.SIMPLE_MATCH_ONLY;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        boolean hasTextField = fieldWeights.keySet().stream().anyMatch(k -> isTextField(context.fieldMapper(k)));
        if (hasTextField == false) {
            return Queries.newUnmappedFieldsQuery(fieldWeights.keySet());
        }
        Analyzer explicitAnalyzer = null;
        if (analyzer != null) {
            explicitAnalyzer = context.getMapperService().getIndexAnalyzers().get(analyzer);
            if (explicitAnalyzer == null) {
                throw new IllegalArgumentException("No analyzer found for [" + analyzer + "]");
            }
        }

        CombinedFieldQuery.Builder builder = new CombinedFieldQuery.Builder();
        boolean hasTerms = false;
        for (Map.Entry<String, Float> fieldWeight : fieldWeights.entrySet()) {
            String fieldName = fieldWeight.getKey();
            MappedFieldType fieldType = context.fieldMapper(fieldName);
            if (isTextField(fieldType) == false) {
                // ignore unmapped fields or fields that do not support text search
                continue;
            }
            builder.addField(fieldName, fieldWeight.getValue());

            Analyzer fieldAnalyzer;
            if (explicitAnalyzer == null) {
                // Use per-field analyzer
                fieldAnalyzer = context.getSearchAnalyzer(fieldType);
            } else {
                fieldAnalyzer = explicitAnalyzer;
            }
            hasTerms = collectAllTerms(fieldName, fieldAnalyzer, value.toString(), builder) || hasTerms;
        }
        if (hasTerms == false) {
            switch (zeroTermsQuery) {
                case NONE:
                    return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
                case ALL:
                    return Queries.newMatchAllQuery();
                default:
                    throw new IllegalStateException("unknown zeroTermsQuery " + zeroTermsQuery);
            }
        }
        return builder.build();
    }

    private static boolean collectAllTerms(String fieldName, Analyzer analyzer, String queryString, CombinedFieldQuery.Builder builder)
        throws IOException {
        boolean hasTerms = false;
        TokenStream tokenStream = analyzer.tokenStream(fieldName, queryString);
        TermToBytesRefAttribute termAtt = tokenStream.addAttribute(TermToBytesRefAttribute.class);
        tokenStream.reset();
        while (tokenStream.incrementToken()) {
            builder.addTerm(BytesRef.deepCopyOf(termAtt.getBytesRef()));
            hasTerms = true;
        }
        tokenStream.close();
        return hasTerms;
    }

    @Override
    protected boolean doEquals(CombinedFieldQueryBuilder other) {
        return Objects.equals(value, other.value)
            && Objects.equals(fieldWeights, other.fieldWeights)
            && Objects.equals(analyzer, other.analyzer);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(value, fieldWeights, analyzer);
    }
}
