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
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.ConstantIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.opensearch.common.lucene.search.AutomatonQueries.caseInsensitivePrefix;
import static org.opensearch.common.lucene.search.AutomatonQueries.toCaseInsensitiveWildcardAutomaton;
import static org.opensearch.index.mapper.TypeParsers.checkNull;
import static org.apache.lucene.search.FuzzyQuery.getFuzzyAutomaton;

/**
 * Index specific field mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "2.14.0")
public class ConstantKeywordFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "constant_keyword";
    private static final String VALUE = "value";
    private static final String VALUES = "values";
    public static final String DOV_VALUES = "doc_values";
    public static final String INDEX = "index";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * A {@link Mapper.TypeParser} for the constant keyword field.
     *
     * @opensearch.internal
     */
    @SuppressWarnings("unchecked")
    public static class TypeParser implements Mapper.TypeParser {
        private final BiFunction<String, ParserContext, Builder> builderFunction;

        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                if (!VALUE.equals(propName) && !VALUES.equals(propNode)) {
                    checkNull(propName, propNode);
                }
                switch (propName) {
                    case VALUE:
                        builder.setValue(XContentMapValues.nodeStringValue(propNode));
                        iterator.remove();
                        break;

                    case VALUES:
                        String[] values = XContentMapValues.nodeStringArrayValue(propNode);
                        Arrays.sort(values);
                        builder.setValues(List.of(values));
                        iterator.remove();
                        break;

                    case DOV_VALUES:
                        builder.setHasDocValues(XContentMapValues.nodeBooleanValue(propNode));
                        iterator.remove();
                        break;
                    case INDEX:
                        builder.setIndexed(XContentMapValues.nodeBooleanValue(propNode));
                        iterator.remove();
                }
            }
            if ((builder.value.getValue() != null ^ builder.sortedValues.getValue() != null) == false) {
                throw new OpenSearchParseException("Field [" + name + "] is missing required parameter [value or values]");
            }

            return builder;
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
        private final Parameter<String> value = Parameter.stringParam(VALUE, false, m -> toType(m).value, null);
        private final Parameter<List<String>> sortedValues = Parameter.stringArrayParam(VALUES, false, m -> toType(m).sortedValues, null);
        private final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).index, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);

        public Builder(String name) {
            super(name);
        }

        @Override
        public List<Parameter<?>> getParameters() {
            return Arrays.asList(value, sortedValues, index, hasDocValues);
        }

        @Override
        public ConstantKeywordFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(KeywordFieldMapper.Defaults.FIELD_TYPE);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(this.index.getValue(), "docs"));

            return new ConstantKeywordFieldMapper(
                name,
                fieldtype,
                hasDocValues.getValue(),
                index.getValue(),
                new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
                    buildFullName(context),
                    value.getValue() != null ? new String[] { value.getValue() } : sortedValues.getValue().toArray(new String[0]),
                    hasDocValues.getValue(),
                    index.getValue()
                ),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }

        public void setValue(String value) {
            this.value.setValue(value);
        }

        public void setValues(List<String> sortedValues) {
            if (sortedValues.size() > 127) {
                throw new IllegalArgumentException("the values of constant_keyword field [" + name + "] must be small than 127 values");
            }
            this.sortedValues.setValue(sortedValues);
        }

        public void setHasDocValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
        }

        public void setIndexed(boolean index) {
            this.index.setValue(index);
        }

    }

    /**
     * Field type for Index field mapper
     *
     * @opensearch.internal
     */
    @PublicApi(since = "2.14.0")
    protected static final class ConstantKeywordFieldType extends MappedFieldType {

        private final Map<String, Integer> keyToOrders;
        private final String[] sortedValues;
        private final KeywordFieldMapper.KeywordFieldType valueFieldType;

        public ConstantKeywordFieldType(String name, String[] sortedValues, boolean hasDocValues, boolean index) {
            super(name, index, false, hasDocValues, TextSearchInfo.NONE, Collections.emptyMap());
            this.keyToOrders = new HashMap<>(sortedValues.length);
            this.sortedValues = sortedValues;
            for (int i = 0; i < sortedValues.length; i++) {
                this.keyToOrders.put(sortedValues[i], i);
            }

            this.valueFieldType = new KeywordFieldMapper.KeywordFieldType(name(), index, hasDocValues, Collections.emptyMap()) {
                @Override
                protected BytesRef indexedValueForSearch(Object value) {
                    if (value == null) {
                        return null;
                    }
                    Integer parsedValue = keyToOrders.get(inputToString(value));
                    return (parsedValue == null ? null : new BytesRef(new byte[] { parsedValue.byteValue() }));
                }
            };
        }

        @Override
        public boolean isSearchable() {
            return valueFieldType.isSearchable();
        }

        @Override
        public boolean hasDocValues() {
            return valueFieldType.hasDocValues();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public Query termQuery(Object value, QueryShardContext context) {
            return buildQuery(String::equals, context, value);
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            return buildQuery((key, input) -> Regex.simpleMatch(input, key, true), context, value);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            return buildQuery(String::equals, context, values.toArray());
        }

        @Override
        public Query prefixQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            if (caseInsensitive) {
                Automaton automaton = caseInsensitivePrefix(value);
                ByteRunAutomaton byteRunAutomaton = new ByteRunAutomaton(automaton);
                return buildQuery((key, input) -> {
                    byte[] bytes = key.getBytes();
                    return byteRunAutomaton.run(bytes, 0, bytes.length);
                }, context, value);
            } else {
                return buildQuery(String::startsWith, context, value);
            }
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            // FuzzyQuery query = new FuzzyQuery(new Term(name(), inputToString(value)), fuzziness.asDistance(BytesRefs.toString(value)),
            // prefixLength, maxExpansions, transpositions);
            // CompiledAutomaton compiledAutomaton = query.getAutomata();
            CompiledAutomaton compiledAutomaton = getFuzzyAutomaton(
                inputToString(value),
                fuzziness.asDistance(BytesRefs.toString(value)),
                prefixLength,
                transpositions
            );
            return buildQuery((key, input) -> {
                byte[] bytes = key.getBytes();
                return compiledAutomaton.runAutomaton.run(bytes, 0, bytes.length);
            }, context, value);
        }

        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            Automaton automaton;
            if (caseInsensitive) {
                automaton = toCaseInsensitiveWildcardAutomaton(new Term(name(), value), Integer.MAX_VALUE);
            } else {
                WildcardQuery wi = new WildcardQuery(new Term(name(), value));
                automaton = wi.getAutomaton();
            }
            ByteRunAutomaton byteRunAutomaton = new ByteRunAutomaton(automaton);

            return buildQuery((key, input) -> {
                byte[] bytes = key.getBytes();
                return byteRunAutomaton.run(bytes, 0, bytes.length);
            }, context, value);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (keyToOrders.size() == 1) {
                return new MatchAllDocsQuery();
            }
            if (isSearchable() || hasDocValues()) {
                return valueFieldType.existsQuery(context);
            } else {
                return new MatchNoDocsQuery();
            }
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
            String lowerTermString = inputToString(lowerTerm);
            String upperTermString = inputToString(upperTerm);

            Query query = checkDisjunction(lowerTermString, upperTermString, includeLower, includeUpper);
            if (query != null) {
                return query;
            }

            List<String> parsedkeys = new ArrayList<>();
            int lowIndex;
            if (lowerTermString != null) {
                lowIndex = findIndex(sortedValues, lowerTermString, true);
                if (lowIndex < 0
                    || sortedValues[lowIndex].equals(lowerTermString) && includeLower == false
                    || sortedValues[lowIndex].compareTo(lowerTermString) < 0) {
                    lowIndex++;
                }
            } else {
                lowIndex = 0;
            }

            int highIndex = -1;
            if (upperTermString != null) {
                highIndex = findIndex(sortedValues, upperTermString, false);
                if (highIndex >= sortedValues.length
                    || sortedValues[highIndex].equals(upperTermString) && includeUpper == false
                    || sortedValues[highIndex].compareTo(upperTermString) > 0) {
                    highIndex--;
                }
            } else {
                highIndex = sortedValues.length - 1;
            }

            for (int i = lowIndex; i <= highIndex; i++) {
                parsedkeys.add(sortedValues[i]);
            }

            if (parsedkeys.isEmpty() || lowIndex > highIndex) {
                return new MatchNoDocsQuery();
            }

            if (parsedkeys.size() == 1 && isSearchable()) {
                return valueFieldType.termQuery(parsedkeys.get(0), context);
            }
            if (parsedkeys.size() == keyToOrders.size()) {
                return existsQuery(context);
            }

            return valueFieldType.termsQuery(parsedkeys, context);
        }

        private static int findIndex(String[] a, String key, boolean notBiggerThan) {
            int low = 0;
            int high = a.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                String midVal = a[mid];
                int cmp = midVal.compareTo(key);

                if (cmp < 0) low = mid + 1;
                else if (cmp > 0) high = mid - 1;
                else return mid; // key found
            }
            if (notBiggerThan) {
                return high;  // key not found.
            } else {
                return low;
            }
        }

        private Query checkDisjunction(String lowerTermString, String upperTermString, boolean includeLower, boolean includeUpper) {
            if (lowerTermString != null && upperTermString != null && (lowerTermString).compareTo(upperTermString) > 0) {
                return new MatchNoDocsQuery();
            }

            if (lowerTermString != null && (lowerTermString).compareTo(sortedValues[sortedValues.length - 1]) > (includeLower ? 0 : -1)) {
                return new MatchNoDocsQuery();
            }

            if (upperTermString != null && (upperTermString).compareTo(sortedValues[0]) < (includeUpper ? 0 : 1)) {
                return new MatchNoDocsQuery();
            }

            if (sortedValues.length == 1) {
                return new MatchAllDocsQuery();
            }
            return null;
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

            return buildQuery((key, input) -> {
                BytesRef valueBytes = BytesRefs.toBytesRef(key);
                return byteRunAutomaton.run(valueBytes.bytes, valueBytes.offset, valueBytes.length);
            }, context, value);
        }

        private Query buildQuery(BiFunction<String, String, Boolean> matcher, QueryShardContext context, Object... values) {
            List<String> parsedkeys = new ArrayList<>();
            for (Object value : values) {
                for (Map.Entry<String, Integer> entry : keyToOrders.entrySet()) {
                    if (matcher.apply(entry.getKey(), inputToString(value))) {
                        parsedkeys.add(entry.getKey());
                    }
                }
            }
            if (parsedkeys.isEmpty()) {
                return new MatchNoDocsQuery();
            }
            if (keyToOrders.size() == 1) {
                return new MatchAllDocsQuery();
            }
            if (parsedkeys.size() == 1 && isSearchable()) {
                return valueFieldType.termQuery(parsedkeys.get(0), context);
            }
            if (parsedkeys.size() == keyToOrders.size()) {
                return existsQuery(context);
            }
            return valueFieldType.termsQuery(parsedkeys, context);
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
    private final Map<String, Integer> keyToOrders = new HashMap<>();
    private final List<String> sortedValues;
    private final boolean index;
    private final boolean hasDocValues;

    public ConstantKeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        boolean hasDocValues,
        boolean index,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        ConstantKeywordFieldMapper.Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.fieldType = fieldType;
        this.hasDocValues = hasDocValues;
        this.index = index;
        this.value = builder.value.getValue();
        this.sortedValues = builder.sortedValues.getValue();
        String lastValue = null;
        if (sortedValues != null) {
            for (int i = 0; i < sortedValues.size(); i++) {
                if (sortedValues.get(i) != lastValue) {
                    keyToOrders.put(sortedValues.get(i), i);
                    lastValue = sortedValues.get(i);
                }
            }
        } else {
            keyToOrders.put(value, 0);
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new ConstantKeywordFieldMapper.Builder(simpleName()).init(this);
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

        if (keyToOrders.containsKey(value) == false) {
            throw new IllegalArgumentException(
                "constant keyword field [" + name() + "] must have a value of [" + this.keyToOrders.keySet() + "]"
            );
        }
        if (keyToOrders.size() > 1) {
            BytesRef bytesRef = new BytesRef(new byte[] { keyToOrders.get(value).byteValue() });

            if (index) {
                Field field = new KeywordFieldMapper.KeywordField(name(), bytesRef, this.fieldType);
                context.doc().add(field);

                if (hasDocValues == false) {
                    createFieldNamesField(context);
                }
            }

            if (hasDocValues) {
                context.doc().add(new SortedSetDocValuesField(fieldType().name(), bytesRef));
            }
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
