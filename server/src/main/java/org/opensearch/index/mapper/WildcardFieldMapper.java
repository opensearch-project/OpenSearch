/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.LeafSearchLookup;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.opensearch.index.mapper.KeywordFieldMapper.normalizeValue;

/**
 * Mapper for the "wildcard" field type, which supports (relatively) efficient matching by wildcard, prefix, and regexp
 * queries. It's not really a "full-text" field type, but rather an "unstructured string" field type.
 *
 * @opensearch.internal
 */
public class WildcardFieldMapper extends ParametrizedFieldMapper {
    private final String nullValue;
    private final int ignoreAbove;
    private final String normalizerName;
    private final boolean hasDocValues;
    private final IndexAnalyzers indexAnalyzers;

    /**
     * The builder for the field mapper.
     *
     * @opensearch.internal
     */
    public static final class Builder extends ParametrizedFieldMapper.Builder {

        // Copy relevant parameters from KeywordFieldMapper
        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValue, null)
            .acceptsNull();
        private final Parameter<Integer> ignoreAbove = Parameter.intParam(
            "ignore_above",
            true,
            m -> toType(m).ignoreAbove,
            Integer.MAX_VALUE
        );
        private final Parameter<String> normalizer = Parameter.stringParam("normalizer", false, m -> toType(m).normalizerName, "default");
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, false);
        private final IndexAnalyzers indexAnalyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
        }

        public Builder(String name) {
            this(name, null);
        }

        public WildcardFieldMapper.Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove.setValue(ignoreAbove);
            return this;
        }

        WildcardFieldMapper.Builder normalizer(String normalizerName) {
            this.normalizer.setValue(normalizerName);
            return this;
        }

        WildcardFieldMapper.Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public WildcardFieldMapper.Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(nullValue, ignoreAbove, normalizer, hasDocValues, meta);
        }

        @Override
        public WildcardFieldMapper build(BuilderContext context) {
            String normalizerName = normalizer.getValue();
            NamedAnalyzer normalizer = Lucene.KEYWORD_ANALYZER;
            if ("default".equals(normalizerName) == false) {
                assert indexAnalyzers != null;
                normalizer = indexAnalyzers.getNormalizer(normalizerName);
            }

            return new WildcardFieldMapper(
                name,
                new WildcardFieldType(context.path().pathAsText(name), normalizer, this),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }

    }

    public static final String CONTENT_TYPE = "wildcard";
    public static final TypeParser PARSER = new TypeParser((n, c) -> new WildcardFieldMapper.Builder(n, c.getIndexAnalyzers()));

    protected WildcardFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.nullValue = builder.nullValue.getValue();
        this.ignoreAbove = builder.ignoreAbove.getValue();
        this.normalizerName = builder.normalizer.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.indexAnalyzers = builder.indexAnalyzers;
    }

    public int ignoreAbove() {
        return ignoreAbove;
    }

    private static final FieldType FIELD_TYPE = new FieldType();
    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.freeze();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else {
                value = parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, name(), value);
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        Tokenizer tokenizer = new WildcardFieldTokenizer();
        tokenizer.setReader(new StringReader(value));
        context.doc().add(new Field(fieldType().name(), tokenizer, FIELD_TYPE));
        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        } else {
            if (fieldType().hasDocValues() == false) {
                createFieldNamesField(context);
            }
        }
    }

    /**
     * Tokenizer to emit tokens to support wildcard first-phase matching.
     * <p>
     * Will emit all substrings of length 1,2, and 3, with 0-valued anchors for the prefix/suffix.
     * <p>
     * For example, given the string "lucene", output the following terms:
     * <p>
     * [0, 'l']
     * [0, 'l', 'u']
     * ['l']
     * ['l', 'u']
     * ['l', 'u', 'c']
     * ['u']
     * ['u','c']
     * ['u','c','e']
     * ['c']
     * ['c', 'e']
     * ['c', 'e', 'n']
     * ['e']
     * ['e', 'n']
     * ['e', 'n', 'e']
     * ['n']
     * ['n', 'e']
     * ['n', 'e', 0]
     * ['e']
     * ['e', 0]
     * <p>
     * Visible for testing.
     */
    static final class WildcardFieldTokenizer extends Tokenizer {
        private final CharTermAttribute charTermAttribute = addAttribute(CharTermAttribute.class);
        private final char[] buffer = new char[3]; // Ring buffer for up to 3 chars
        private int offset = 0; // Position in the buffer
        private int length = 2; // First token is anchor + first char

        @Override
        public void reset() throws IOException {
            super.reset();
            buffer[0] = 0;
            int firstChar = input.read();
            if (firstChar != -1) {
                buffer[1] = (char) firstChar;
                int secondChar = input.read();
                if (secondChar != -1) {
                    buffer[2] = (char) secondChar;
                } else {
                    buffer[2] = 0;
                }
            } else {
                buffer[1] = 0;
            }

        }

        @Override
        public boolean incrementToken() throws IOException {
            charTermAttribute.setLength(length);
            int numZeroes = 0;
            for (int i = 0; i < length; i++) {
                char curChar = buffer[(i + offset) % 3];
                if (curChar == 0) {
                    numZeroes++;
                }
                charTermAttribute.buffer()[i] = buffer[(i + offset) % 3];
            }
            if (numZeroes == 2) {
                // Two zeroes usually means we're done.
                if (length == 3 && charTermAttribute.buffer()[1] != 0) {
                    // The only case where we're not done is if the input has exactly 1 character, so the buffer
                    // contains 0, char, 0. In that case, we return char now, then return char, 0 on the next iteration
                    charTermAttribute.buffer()[0] = charTermAttribute.buffer()[1];
                    charTermAttribute.buffer()[1] = 0;
                    charTermAttribute.setLength(1);
                    length = 2;
                    offset = 1;
                    return true;
                }
                return false;
            }
            if (length == 3) {
                // Read the next character, overwriting the current offset
                int nextChar = input.read();
                if (nextChar != -1) {
                    buffer[offset] = (char) nextChar;
                } else {
                    // End of input. Pad with extra 0 to trigger the logic above.
                    buffer[offset] = 0;
                }
                offset = (offset + 1) % 3;
                length = 1;
            } else {
                length = length + 1;
            }
            return true;
        }
    }

    /**
     * Implements the various query types over wildcard fields.
     */
    public static final class WildcardFieldType extends StringFieldType {
        private final int ignoreAbove;
        private final String nullValue;

        public WildcardFieldType(String name) {
            this(name, Collections.emptyMap());
        }

        public WildcardFieldType(String name, Map<String, String> meta) {
            super(name, true, false, false, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
        }

        public WildcardFieldType(String name, NamedAnalyzer normalizer, Builder builder) {
            super(name, true, true, builder.hasDocValues.getValue(), TextSearchInfo.SIMPLE_MATCH_ONLY, builder.meta.getValue());
            setIndexAnalyzer(normalizer);
            this.ignoreAbove = builder.ignoreAbove.getValue();
            this.nullValue = builder.nullValue.getValue();
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // Copied from KeywordFieldMapper.KeywordFieldType
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't " + "support formats.");
            }

            if (hasDocValues()) {
                return new DocValueFetcher(DocValueFormat.RAW, searchLookup.doc().getForField(this));
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    if (keywordValue.length() > ignoreAbove) {
                        return null;
                    }

                    NamedAnalyzer normalizer = normalizer();
                    if (normalizer == null) {
                        return keywordValue;
                    }

                    try {
                        return normalizeValue(normalizer, name(), keywordValue);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        NamedAnalyzer normalizer() {
            return indexAnalyzer();
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            QueryShardContext context
        ) {
            // TODO: Not sure if we can reasonably describe a fuzzy query in terms of n-grams without exploding the cardinality
            throw new IllegalArgumentException(
                "Can only use fuzzy queries on keyword and text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
            );
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            return wildcardQuery(value + "*", method, caseInsensitive, context);
        }

        @Override
        public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            NamedAnalyzer normalizer = normalizer();
            if (normalizer != null) {
                value = normalizeWildcardPattern(name(), value, normalizer);
            }
            final String finalValue;
            if (caseInsensitive) {
                // Use ROOT locale, as it seems to be consistent with AutomatonQueries.toCaseInsensitiveChar.
                finalValue = value.toLowerCase(Locale.ROOT);
            } else {
                finalValue = value;
            }
            Predicate<String> matchPredicate;
            Automaton automaton = WildcardQuery.toAutomaton(new Term(name(), finalValue));
            CompiledAutomaton compiledAutomaton = new CompiledAutomaton(automaton);
            if (compiledAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.SINGLE) {
                // when type equals SINGLE, #compiledAutomaton.runAutomaton is null
                matchPredicate = s -> {
                    if (caseInsensitive) {
                        s = s.toLowerCase(Locale.ROOT);
                    }
                    return s.equals(finalValue);
                };
            } else if (compiledAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.ALL) {
                return existsQuery(context);
            } else if (compiledAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.NONE) {
                return new MatchNoDocsQuery("Wildcard expression matches nothing");
            } else {
                matchPredicate = s -> {
                    if (caseInsensitive) {
                        s = s.toLowerCase(Locale.ROOT);
                    }
                    BytesRef valueBytes = BytesRefs.toBytesRef(s);
                    return compiledAutomaton.runAutomaton.run(valueBytes.bytes, valueBytes.offset, valueBytes.length);
                };
            }

            Set<String> requiredNGrams = getRequiredNGrams(finalValue);
            Query approximation;
            if (requiredNGrams.isEmpty()) {
                // This only happens when all characters are wildcard characters (* or ?),
                // or it's the empty string.
                if (value.length() == 0 || value.contains("?")) {
                    approximation = this.existsQuery(context);
                } else {
                    return existsQuery(context);
                }
            } else {
                approximation = matchAllTermsQuery(name(), requiredNGrams);
            }
            return new WildcardMatchingQuery(name(), approximation, matchPredicate, value, context, this);
        }

        // Package-private for testing
        static Set<String> getRequiredNGrams(String value) {
            Set<String> terms = new HashSet<>();

            if (value.isEmpty()) {
                return terms;
            }

            int pos = 0;
            String rawSequence = null;
            String currentSequence = null;
            if (!value.startsWith("?") && !value.startsWith("*")) {
                // Can add prefix term
                rawSequence = getNonWildcardSequence(value, 0);
                currentSequence = performEscape(rawSequence);
                if (currentSequence.length() == 1) {
                    terms.add(new String(new char[]{0, currentSequence.charAt(0)}));
                } else {
                    terms.add(new String(new char[]{0, currentSequence.charAt(0), currentSequence.charAt(1)}));
                }
            } else {
                pos = findNonWildcardSequence(value, pos);
                rawSequence = getNonWildcardSequence(value, pos);
            }
            while (pos < value.length()) {
                boolean isEndOfValue = pos + rawSequence.length() == value.length();
                currentSequence = performEscape(rawSequence);
                if (!currentSequence.isEmpty() && currentSequence.length() < 3 && !isEndOfValue && pos > 0) {
                    // If this is a prefix or suffix of length < 3, then we already have a longer token including the anchor.
                    terms.add(currentSequence);
                } else {
                    for (int i = 0; i < currentSequence.length() - 2; i++) {
                        terms.add(currentSequence.substring(i, i + 3));
                    }
                }
                if (isEndOfValue) {
                    // This is the end of the input. We can attach a suffix anchor.
                    if (currentSequence.length() == 1) {
                        terms.add(new String(new char[]{currentSequence.charAt(0), 0}));
                    } else {
                        char a = currentSequence.charAt(currentSequence.length() - 2);
                        char b = currentSequence.charAt(currentSequence.length() - 1);
                        terms.add(new String(new char[]{a, b, 0}));
                    }
                }
                pos = findNonWildcardSequence(value, pos + rawSequence.length());
                rawSequence = getNonWildcardSequence(value, pos);
            }
            return terms;
        }

        private static String getNonWildcardSequence(String value, int startFrom) {
            for (int i = startFrom; i < value.length(); i++) {
                char c = value.charAt(i);
                if ((c == '?' || c == '*') &&
                    (i == 0 || value.charAt(i - 1) != '\\')) {
                    return value.substring(startFrom, i);
                }
            }
            // Made it to the end. No more wildcards.
            return value.substring(startFrom);
        }

        private static int findNonWildcardSequence(String value, int startFrom) {
            for (int i = startFrom; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c != '?' && c != '*') {
                    return i;
                }
            }
            return value.length();
        }

        private static String performEscape(String str) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < str.length(); i++) {
                if (str.charAt(i) == '\\' && (i + 1) < str.length()) {
                    char c = str.charAt(i + 1);
                    if (c == '*' || c == '?') {
                        i++;
                    }
                }
                sb.append(str.charAt(i));
            }
            assert !sb.toString().contains("\\*");
            assert !sb.toString().contains("\\?");
            return sb.toString();
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
            NamedAnalyzer normalizer = normalizer();
            if (normalizer != null) {
                value = normalizer.normalize(name(), value).utf8ToString();
            }

            RegExp regExp = new RegExp(value, syntaxFlags, matchFlags);
            Automaton automaton = regExp.toAutomaton(maxDeterminizedStates);
            CompiledAutomaton compiledAutomaton = new CompiledAutomaton(automaton);

            Predicate<String> regexpPredicate;
            if (compiledAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.ALL) {
                return existsQuery(context);
            } else if (compiledAutomaton.type == CompiledAutomaton.AUTOMATON_TYPE.NONE) {
                return new MatchNoDocsQuery("Regular expression matches nothing");
            } else {
                regexpPredicate = s -> {
                    BytesRef valueBytes = BytesRefs.toBytesRef(s);
                    return compiledAutomaton.runAutomaton.run(valueBytes.bytes, valueBytes.offset, valueBytes.length);
                };
            }

            Query approximation = regexpToQuery(name(), regExp);
            if (approximation instanceof MatchAllDocsQuery) {
                approximation = existsQuery(context);
            }
            return new WildcardMatchingQuery(name(), approximation, regexpPredicate, "/" + value + "/", context, this);
        }

        /**
         * Implement the match rules described in <a href="https://swtch.com/~rsc/regexp/regexp4.html">Regular Expression Matching with a Trigram Index</a>.
         *
         * @param fieldName name of the wildcard field
         * @param regExp a parsed node in the {@link RegExp} tree
         * @return a query that matches on the known required parts of the given regular expression
         */
        private static Query regexpToQuery(String fieldName, RegExp regExp) {
            BooleanQuery query;
            if (Objects.requireNonNull(regExp.kind) == RegExp.Kind.REGEXP_UNION) {
                List<Query> clauses = new ArrayList<>();
                while (regExp.exp1.kind == RegExp.Kind.REGEXP_UNION) {
                    clauses.add(regexpToQuery(fieldName, regExp.exp2));
                    regExp = regExp.exp1;
                }
                clauses.add(regexpToQuery(fieldName, regExp.exp2));
                clauses.add(regexpToQuery(fieldName, regExp.exp1));
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (int i = clauses.size() - 1; i >= 0; i--) {
                    Query clause = clauses.get(i);
                    if (clause instanceof MatchAllDocsQuery) {
                        return clause;
                    }
                    builder.add(clause, BooleanClause.Occur.SHOULD);
                }
                query = builder.build();
            } else if (regExp.kind == RegExp.Kind.REGEXP_STRING) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (String string : getRequiredNGrams("*" + regExp.s + "*")) {
                    builder.add(new TermQuery(new Term(fieldName, string)), BooleanClause.Occur.FILTER);
                }
                query = builder.build();
            } else if (regExp.kind == RegExp.Kind.REGEXP_CONCATENATION) {
                List<Query> clauses = new ArrayList<>();
                while (regExp.exp1.kind == RegExp.Kind.REGEXP_CONCATENATION) {
                    clauses.add(regexpToQuery(fieldName, regExp.exp2));
                    regExp = regExp.exp1;
                }
                clauses.add(regexpToQuery(fieldName, regExp.exp2));
                clauses.add(regexpToQuery(fieldName, regExp.exp1));
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                for (int i = clauses.size() - 1; i >= 0; i--) {
                    Query clause = clauses.get(i);
                    if (!(clause instanceof MatchAllDocsQuery)) {
                        builder.add(clause, BooleanClause.Occur.FILTER);
                    }
                }
                query = builder.build();
            } else if ((regExp.kind == RegExp.Kind.REGEXP_REPEAT_MIN || regExp.kind == RegExp.Kind.REGEXP_REPEAT_MINMAX)
                && regExp.min > 0) {
                return regexpToQuery(fieldName, regExp.exp1);
            } else {
                return new MatchAllDocsQuery();
            }
            if (query.clauses().size() == 1) {
                return query.iterator().next().getQuery();
            } else if (query.clauses().size() == 0) {
                return new MatchAllDocsQuery();
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            return wildcardQuery(BytesRefs.toString(value), MultiTermQuery.CONSTANT_SCORE_REWRITE, true, context);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return wildcardQuery(BytesRefs.toString(value), MultiTermQuery.CONSTANT_SCORE_REWRITE, false, context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            Set<String> expectedValues = new HashSet<>();
            StringBuilder pattern = new StringBuilder();
            for (Object value : values) {
                String stringVal = BytesRefs.toString(value);
                builder.add(matchAllTermsQuery(name(), getRequiredNGrams(stringVal)), BooleanClause.Occur.SHOULD);
                expectedValues.add(stringVal);
                if (pattern.length() > 0) {
                    pattern.append('|');
                }
                pattern.append(stringVal);
            }
            return new WildcardMatchingQuery(name(), builder.build(), expectedValues::contains, pattern.toString(), context, this);
        }

        private static BooleanQuery matchAllTermsQuery(String fieldName, Set<String> terms) {
            BooleanQuery.Builder matchAllTermsBuilder = new BooleanQuery.Builder();
            for (String term : terms) {
                matchAllTermsBuilder.add(new TermQuery(new Term(fieldName, term)), BooleanClause.Occur.FILTER);
            }
            return matchAllTermsBuilder.build();
        }
    }

    /**
     * Custom two-phase query type for queries over the wildcard field. The expected behavior is that a first-phase
     * query provides the best possible filter over the indexed trigrams, while the second phase matcher eliminates
     * false positives by evaluating the true field value.
     */
    static class WildcardMatchingQuery extends Query {
        private static final long MATCH_COST_ESTIMATE = 1000L;
        private final String fieldName;
        private final Query firstPhaseQuery;
        private final Predicate<String> secondPhaseMatcher;
        private final String patternString; // For toString
        private final ValueFetcher valueFetcher;
        private final SearchLookup searchLookup;

        WildcardMatchingQuery(String fieldName, Query firstPhaseQuery, String patternString) {
            this(fieldName, firstPhaseQuery, s -> true, patternString, (QueryShardContext) null, null);
        }

        public WildcardMatchingQuery(
            String fieldName,
            Query firstPhaseQuery,
            Predicate<String> secondPhaseMatcher,
            String patternString,
            QueryShardContext context,
            WildcardFieldType fieldType
        ) {
            this.fieldName = Objects.requireNonNull(fieldName);
            this.firstPhaseQuery = Objects.requireNonNull(firstPhaseQuery);
            this.secondPhaseMatcher = Objects.requireNonNull(secondPhaseMatcher);
            this.patternString = Objects.requireNonNull(patternString);
            if (context != null) {
                this.searchLookup = context.lookup();
                this.valueFetcher = fieldType.valueFetcher(context, context.lookup(), null);
            } else {
                this.searchLookup = null;
                this.valueFetcher = null;
            }
        }

        private WildcardMatchingQuery(
            String fieldName,
            Query firstPhaseQuery,
            Predicate<String> secondPhaseMatcher,
            String patternString,
            ValueFetcher valueFetcher,
            SearchLookup searchLookup
        ) {
            this.fieldName = fieldName;
            this.firstPhaseQuery = firstPhaseQuery;
            this.secondPhaseMatcher = secondPhaseMatcher;
            this.patternString = patternString;
            this.valueFetcher = valueFetcher;
            this.searchLookup = searchLookup;
        }

        @Override
        public String toString(String s) {
            return "WildcardMatchingQuery(" + fieldName + ":\"" + patternString + "\")";
        }

        @Override
        public void visit(QueryVisitor queryVisitor) {
            firstPhaseQuery.visit(queryVisitor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WildcardMatchingQuery that = (WildcardMatchingQuery) o;
            return Objects.equals(fieldName, that.fieldName)
                && Objects.equals(firstPhaseQuery, that.firstPhaseQuery)
                && Objects.equals(patternString, that.patternString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, firstPhaseQuery, patternString);
        }

        @Override
        public Query rewrite(IndexSearcher indexSearcher) throws IOException {
            Query rewriteFirstPhase = firstPhaseQuery.rewrite(indexSearcher);
            if (rewriteFirstPhase != firstPhaseQuery) {
                return new WildcardMatchingQuery(
                    fieldName,
                    rewriteFirstPhase,
                    secondPhaseMatcher,
                    patternString,
                    valueFetcher,
                    searchLookup
                );
            }
            return this;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Weight firstPhaseWeight = firstPhaseQuery.createWeight(searcher, scoreMode, boost);
            return new ConstantScoreWeight(this, boost) {
                @Override
                public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                    ScorerSupplier supplier = scorerSupplier(leafReaderContext);
                    if (supplier == null) {
                        return null;
                    }
                    return supplier.get(Long.MAX_VALUE);
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    Weight weight = this;
                    ScorerSupplier firstPhaseSupplier = firstPhaseWeight.scorerSupplier(context);
                    if (firstPhaseSupplier == null) {
                        return null;
                    }
                    return new ScorerSupplier() {
                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            Scorer approximateScorer = firstPhaseSupplier.get(leadCost);
                            DocIdSetIterator approximation = approximateScorer.iterator();
                            LeafSearchLookup leafSearchLookup = searchLookup.getLeafSearchLookup(context);
                            valueFetcher.setNextReader(context);

                            TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(approximation) {
                                @Override
                                public boolean matches() throws IOException {
                                    leafSearchLookup.setDocument(approximation.docID());
                                    List<?> values = valueFetcher.fetchValues(leafSearchLookup.source());
                                    for (Object value : values) {
                                        if (secondPhaseMatcher.test(value.toString())) {
                                            return true;
                                        }
                                    }
                                    return false;
                                }

                                @Override
                                public float matchCost() {
                                    return MATCH_COST_ESTIMATE;
                                }
                            };
                            return new ConstantScoreScorer(weight, score(), scoreMode, twoPhaseIterator);
                        }

                        @Override
                        public long cost() {
                            long firstPhaseCost = firstPhaseSupplier.cost();
                            if (firstPhaseCost >= Long.MAX_VALUE / MATCH_COST_ESTIMATE) {
                                return Long.MAX_VALUE;
                            }
                            return firstPhaseCost * MATCH_COST_ESTIMATE;
                        }
                    };
                }

                @Override
                public boolean isCacheable(LeafReaderContext leafReaderContext) {
                    return true;
                }
            };
        }

        // Visible for testing
        Predicate<String> getSecondPhaseMatcher() {
            return secondPhaseMatcher;
        }
    }

    @Override
    public WildcardFieldType fieldType() {
        return (WildcardFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexAnalyzers).init(this);
    }

    private static WildcardFieldMapper toType(FieldMapper in) {
        return (WildcardFieldMapper) in;
    }
}
