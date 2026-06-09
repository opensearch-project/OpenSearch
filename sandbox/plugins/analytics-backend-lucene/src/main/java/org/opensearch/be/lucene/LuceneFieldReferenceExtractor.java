/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.analytics.spi.FieldReferenceExtractor;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene implementation of {@link FieldReferenceExtractor} for the multi-field relevance functions
 * ({@code query_string}, {@code simple_query_string}, {@code multi_match}).
 *
 * <p>Explicit field tokens and the query string are read via {@link ConversionUtils#extractRelevanceOperands}
 * — the exact operand-extraction path {@link org.opensearch.be.lucene.serializers.AbstractRelevanceSerializer}
 * uses to build the executed {@link org.opensearch.index.query.QueryBuilder} — so extraction cannot drift
 * from execution and field order is preserved (first appearance).
 *
 * <p>For {@code query_string} only, the query string is additionally parsed with Lucene's classic
 * grammar (using a no-op analyzer) to collect fields written inside it (e.g. {@code category:A},
 * {@code age:>=10}, {@code _exists_:status}). Terms with no field qualifier set
 * {@link FieldReferences#defaultFieldFanout()}. No {@code QueryShardContext} is needed: field
 * identity is analyzer-independent.
 *
 * <p>Tokens are classified literal vs. pattern via {@link Regex#isSimpleMatchPattern} — the same
 * predicate OpenSearch's resolver uses — so plan-time classification matches runtime. Only literals
 * are validated by the planner; patterns and fan-out pass through.
 *
 * @opensearch.internal
 */
final class LuceneFieldReferenceExtractor implements FieldReferenceExtractor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFieldReferenceExtractor.class);

    /** Field name for the EXISTS pseudo-operator: {@code _exists_:status} references field {@code status}. */
    private static final String EXISTS_FIELD = "_exists_";

    /**
     * Sentinel default field for parsing {@code query_string} bodies. Unqualified terms resolve to
     * this field; seeing it marks {@link FieldReferences#defaultFieldFanout()} rather than emitting a
     * field. Uses a control char so it can never collide with a real mapping field name.
     */
    private static final String DEFAULT_FIELD_SENTINEL = "\u0000__analytics_default_field__";

    /** First operand index at which optional key/value param MAPs begin (after {@code fields} and {@code query}). */
    private static final int OPTIONAL_PARAMS_START_INDEX = 2;

    private final String functionName;
    private final boolean parseQueryString;

    /**
     * @param functionName     the relevance function name, for error messages (e.g. {@code "query_string"})
     * @param parseQueryString whether to parse the query string for in-string fields (true only for
     *                         {@code query_string}; {@code simple_query_string}/{@code multi_match} have no
     *                         in-string field syntax)
     */
    LuceneFieldReferenceExtractor(String functionName, boolean parseQueryString) {
        this.functionName = functionName;
        this.parseQueryString = parseQueryString;
    }

    @Override
    public FieldReferences referencedFields(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);

        // Collect referenced field tokens in first-appearance order, deduped.
        Set<String> tokens = new LinkedHashSet<>();

        // 1. Explicit fields from the `fields`/`field` operand (already mapping-resolved for $ref forms).
        if (operands.fields() != null) {
            tokens.addAll(operands.fields());
        }
        if (operands.fieldName() != null) {
            tokens.add(operands.fieldName());
        }
        boolean hasExplicitFields = tokens.isEmpty() == false;

        // 2. For query_string, fields written inside the query string itself.
        boolean[] sawUnqualified = { false };
        if (parseQueryString && operands.query() != null) {
            collectInStringFields(operands.query(), tokens, sawUnqualified);
        }

        // Default-field fan-out applies only when no explicit `fields` are given. With explicit
        // fields, unqualified terms search those fields rather than fanning out to default_field.
        boolean defaultFieldFanout;
        if (hasExplicitFields) {
            defaultFieldFanout = false;
        } else if (parseQueryString) {
            defaultFieldFanout = sawUnqualified[0] || tokens.isEmpty();
        } else {
            // simple_query_string / multi_match with no explicit fields: rely on default_field.
            defaultFieldFanout = true;
        }

        // 3. Classify literal vs. pattern.
        List<String> literalFields = new ArrayList<>();
        List<String> patternTokens = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                patternTokens.add(token);
            } else {
                literalFields.add(token);
            }
        }

        boolean lenient = resolveLenient(call);
        return new FieldReferences(literalFields, patternTokens, defaultFieldFanout, lenient);
    }

    /**
     * Parses the query string with Lucene's classic grammar and records every field a leaf clause
     * touches. The sentinel default field marks fan-out; {@code _exists_:field} contributes the
     * named field (its term text) rather than {@code _exists_}.
     *
     * <p>Best-effort: the plan-time parser does not implement OpenSearch's {@code query_string}
     * extensions (e.g. {@code field:>15} unbounded ranges). On any parse failure, in-string
     * extraction is skipped and validation relies on the explicit {@code fields} operand — the
     * authoritative parse happens at execution (see design decision 4).
     */
    private void collectInStringFields(String queryString, Set<String> tokens, boolean[] sawUnqualified) {
        QueryParser parser = new QueryParser(DEFAULT_FIELD_SENTINEL, Lucene.KEYWORD_ANALYZER);
        parser.setAllowLeadingWildcard(true);
        Query query;
        try {
            query = parser.parse(queryString);
        } catch (ParseException | RuntimeException e) {
            LOGGER.debug(
                "[{}] skipping in-string field extraction; query string not parseable by plan-time parser [{}]: {}",
                functionName,
                queryString,
                e.getMessage()
            );
            return;
        }
        query.visit(new QueryVisitor() {
            @Override
            public boolean acceptField(String field) {
                if (DEFAULT_FIELD_SENTINEL.equals(field)) {
                    sawUnqualified[0] = true;
                } else if (EXISTS_FIELD.equals(field) == false) {
                    // _exists_ is handled via its term text in consumeTerms below.
                    tokens.add(field);
                }
                return true;
            }

            @Override
            public void consumeTerms(Query q, Term... terms) {
                for (Term term : terms) {
                    if (EXISTS_FIELD.equals(term.field())) {
                        tokens.add(term.text());
                    }
                }
            }
        });
    }

    /**
     * Effective lenient flag: the explicit {@code lenient} param when present, otherwise {@code false}
     * (assume non-lenient when unset — see design decision 3, Option B). Treating unset as non-lenient
     * preserves the planner's eager rejection of explicitly-named non-text fields; an explicit
     * {@code lenient=true} opts out. No index-setting lookup in this cut.
     */
    private boolean resolveLenient(RexCall call) {
        Map<String, String> params = ConversionUtils.extractOptionalParams(call, OPTIONAL_PARAMS_START_INDEX);
        String value = params.get("lenient");
        return value != null && Boolean.parseBoolean(value);
    }
}
