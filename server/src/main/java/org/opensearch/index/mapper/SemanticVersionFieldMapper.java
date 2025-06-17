/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A field mapper for a semantic version field
 *
 */
public class SemanticVersionFieldMapper extends ParametrizedFieldMapper {
    public static final String CONTENT_TYPE = "version";
    public static final FieldType FIELD_TYPE = new FieldType();
    public static final String NORMALIZED_FIELD_SUFFIX = "._normalized";

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setStored(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setDocValuesType(DocValuesType.SORTED_SET);
        FIELD_TYPE.freeze();
    }

    private final Map<String, String> meta;

    protected SemanticVersionFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Map<String, String> meta
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.meta = meta;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
    }

    /**
     * Builder class for constructing the SemanticVersionFieldMapper.
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> true, true);

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            List<Parameter<?>> parameters = new ArrayList<>();
            parameters.add(meta);
            parameters.add(docValues);
            return parameters;
        }

        @Override
        public SemanticVersionFieldMapper build(BuilderContext context) {
            return new SemanticVersionFieldMapper(
                name,
                FIELD_TYPE,
                new SemanticVersionFieldType(buildFullName(context), meta.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                meta.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * The specific field type for SemanticVersionFieldMapper
     *
     * @opensearch.internal
     */
    public static class SemanticVersionFieldType extends TermBasedFieldType {
        private final Map<String, String> meta;
        private final String normalizedFieldName;

        public SemanticVersionFieldType(String name, Map<String, String> meta) {
            super(name, true, true, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.meta = meta;
            this.normalizedFieldName = name + ".normalized";
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new FieldExistsQuery(name());
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            if (value == null) {
                throw new IllegalArgumentException("Cannot search for null value");
            }

            BytesRef bytes;
            if (value instanceof BytesRef) {
                bytes = (BytesRef) value;
            } else {
                bytes = new BytesRef(value.toString());
            }

            return new TermQuery(new Term(name(), bytes));
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {

            try {
                BytesRef lower = null;
                BytesRef upper = null;

                if (lowerTerm != null) {
                    String lowerStr = (lowerTerm instanceof BytesRef) ? ((BytesRef) lowerTerm).utf8ToString() : lowerTerm.toString();
                    SemanticVersion lowerVersion = SemanticVersion.parse(lowerStr);
                    lower = new BytesRef(lowerVersion.getNormalizedComparableString());
                }

                if (upperTerm != null) {
                    String upperStr = (upperTerm instanceof BytesRef) ? ((BytesRef) upperTerm).utf8ToString() : upperTerm.toString();
                    SemanticVersion upperVersion = SemanticVersion.parse(upperStr);
                    upper = new BytesRef(upperVersion.getNormalizedComparableString());
                }

                return new TermRangeQuery(name() + NORMALIZED_FIELD_SUFFIX, lower, upper, includeLower, includeUpper);

            } catch (Exception e) {
                throw new QueryShardException(
                    context,
                    "Failed to create range query for field ["
                        + name()
                        + "]. Lower term: ["
                        + (lowerTerm != null ? lowerTerm.toString() : "null")
                        + "], Upper term: ["
                        + (upperTerm != null ? upperTerm.toString() : "null")
                        + "]"
                );
            }
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {

            if (method == null) {
                method = MultiTermQuery.CONSTANT_SCORE_REWRITE; // default rewrite method
            }

            return new RegexpQuery(
                new Term(name(), indexedValueForSearch(value)),
                syntaxFlags,
                matchFlags,
                RegexpQuery.DEFAULT_PROVIDER,
                maxDeterminizedStates,
                method
            );
        }

        @Override
        public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            if (caseInsensitive) {
                value = value.toLowerCase(Locale.ROOT);
            }
            return new WildcardQuery(new Term(name(), indexedValueForSearch(value)), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            if (method == null) {
                method = MultiTermQuery.CONSTANT_SCORE_REWRITE; // Default rewrite method
            }
            return new PrefixQuery(new Term(name(), indexedValueForSearch(value)), method);
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {

            if (method == null) {
                method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
            }

            return new FuzzyQuery(
                new Term(name(), indexedValueForSearch(value)),
                fuzziness.asDistance(),
                prefixLength,
                maxExpansions,
                transpositions,
                method
            );
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return new SourceValueFetcher(name(), context, format) {
                @Override
                protected String parseSourceValue(Object value) {
                    return value.toString();
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            return new SortedSetOrdinalsIndexFieldData.Builder(name() + "._normalized", CoreValuesSourceType.BYTES);
        }

        @Override
        public Map<String, String> meta() {
            return meta;
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value = context.parser().textOrNull();
        if (value == null) {
            return;
        }

        SemanticVersion version = SemanticVersion.parse(value);
        String versionString = version.toString();
        String normalizedValue = version.getNormalizedComparableString();
        BytesRef bytes = new BytesRef(versionString);
        BytesRef normalizedValueBytes = new BytesRef(normalizedValue);

        // For retrieval: store original version string
        context.doc().add(new StoredField(fieldType().name(), versionString));

        // For searching (term queries): use original version string
        context.doc().add(new KeywordField(fieldType().name(), bytes, Field.Store.YES));

        // For range queries and sorting: use normalized form
        context.doc().add(new KeywordField(fieldType().name() + NORMALIZED_FIELD_SUFFIX, normalizedValueBytes, Field.Store.NO));
        context.doc().add(new SortedSetDocValuesField(fieldType().name() + NORMALIZED_FIELD_SUFFIX, normalizedValueBytes));
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        Builder builder = new Builder(name());
        builder.init(this);
        return builder;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
