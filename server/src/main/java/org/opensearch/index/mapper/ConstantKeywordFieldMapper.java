/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.ConstantIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Index specific field mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class ConstantKeywordFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "constant_keyword";

    private static final String valuePropertyName = "value";

    /**
     * A {@link Mapper.TypeParser} for the constant keyword field.
     *
     * @opensearch.internal
     */
    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            if (!node.containsKey(valuePropertyName)) {
                throw new OpenSearchParseException("Field [" + name + "] is missing required parameter [value]");
            }
            Object value = node.remove(valuePropertyName);
            if (!(value instanceof String)) {
                throw new OpenSearchParseException("Field [" + name + "] is expected to be a string value");
            }
            return new Builder(name, (String) value);
        }
    }

    private static ConstantKeywordFieldMapper toType(FieldMapper in) {
        return (ConstantKeywordFieldMapper) in;
    }

    /**
     * Builder for the binary field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<String> value = Parameter.stringParam(valuePropertyName, false, m -> toType(m).value, null);

        public Builder(String name, String value) {
            super(name);
            this.value.setValue(value);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(value);
        }

        @Override
        public ConstantKeywordFieldMapper build(BuilderContext context) {
            return new ConstantKeywordFieldMapper(
                name,
                new ConstantKeywordFieldMapper.ConstantKeywordFieldType(buildFullName(context), value.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    /**
     * Field type for Index field mapper
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    protected static final class ConstantKeywordFieldType extends ConstantFieldType {

        protected final String value;

        public ConstantKeywordFieldType(String name, String value) {
            super(name, Collections.emptyMap());
            this.value = value;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        protected boolean matches(String pattern, boolean caseInsensitive, QueryShardContext context) {
            return Regex.simpleMatch(pattern, value, caseInsensitive);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return new MatchAllDocsQuery();
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            QueryShardContext context
        ) {
            if (lowerTerm != null) {
                lowerTerm = valueToString(lowerTerm);
            }
            if (upperTerm != null) {
                upperTerm = valueToString(upperTerm);
            }

            if (lowerTerm != null && upperTerm != null && ((String) lowerTerm).compareTo((String) upperTerm) > 0) {
                return new MatchNoDocsQuery();
            }

            if (lowerTerm != null && ((String) lowerTerm).compareTo(value) > (includeLower ? 0 : -1)) {
                return new MatchNoDocsQuery();
            }

            if (upperTerm != null && ((String) upperTerm).compareTo(value) < (includeUpper ? 0 : 1)) {
                return new MatchNoDocsQuery();
            }
            return new MatchAllDocsQuery();
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            Automaton automaton = new RegExp(value, syntaxFlags, matchFlags).toAutomaton(
                RegexpQuery.DEFAULT_PROVIDER,
                maxDeterminizedStates
            );
            ByteRunAutomaton byteRunAutomaton = new ByteRunAutomaton(automaton);
            BytesRef valueBytes = BytesRefs.toBytesRef(this.value);
            if (byteRunAutomaton.run(valueBytes.bytes, valueBytes.offset, valueBytes.length)) {
                return new MatchAllDocsQuery();
            } else {
                return new MatchNoDocsQuery();
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            return new ConstantIndexFieldData.Builder(fullyQualifiedIndexName, name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't " + "support formats.");
            }

            return new SourceValueFetcher(name(), context) {
                @Override
                protected Object parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    return Collections.singletonList(keywordValue);
                }
            };
        }
    }

    private final String value;

    protected ConstantKeywordFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        ConstantKeywordFieldMapper.Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.value = builder.value.getValue();
    }

    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ConstantKeywordFieldMapper.Builder(simpleName(), this.value).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {

        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }
        if (value == null) {
            throw new IllegalArgumentException("constant keyword field [" + name() + "] must have a value");
        }

        if (!value.equals(fieldType().value)) {
            throw new IllegalArgumentException("constant keyword field [" + name() + "] must have a value of [" + this.value + "]");
        }

    }

    @Override
    public ConstantKeywordFieldMapper.ConstantKeywordFieldType fieldType() {
        return (ConstantKeywordFieldMapper.ConstantKeywordFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
