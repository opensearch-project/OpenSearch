/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives the {@link FieldReferences} a multi-field relevance predicate references, for the planner's
 * plan-time field-type validation. Kept separate from the serializers so the serializer base stays
 * focused on building {@link org.opensearch.index.query.QueryBuilder}s; the relevance serializers
 * simply delegate their {@code referencedFields} to this helper.
 *
 * <p>Explicit field tokens come from the already-parsed {@link ConversionUtils.RelevanceOperands}
 * (the same operands the serializer builds its query from, so extraction cannot drift from execution).
 * For {@code query_string} only, the query string is additionally parsed with Lucene's classic grammar
 * (no-op analyzer) to collect fields written inside it (e.g. {@code category:A}, {@code _exists_:status}).
 * Tokens are classified literal vs. pattern via {@link Regex#isSimpleMatchPattern} — the same predicate
 * OpenSearch's resolver uses — so plan-time classification matches runtime. Only literals are validated
 * by the planner; patterns and fan-out pass through.
 *
 * @opensearch.internal
 */
final class RelevanceFieldExtractor {

    private static final Logger LOGGER = LogManager.getLogger(RelevanceFieldExtractor.class);

    /** Field name for the EXISTS pseudo-operator: {@code _exists_:status} references field {@code status}. */
    private static final String EXISTS_FIELD = "_exists_";

    /**
     * Sentinel default field for parsing {@code query_string} bodies. Unqualified terms resolve to
     * this field; it is skipped rather than emitted as a reference. Uses a control char so it can
     * never collide with a real mapping field name.
     */
    private static final String DEFAULT_FIELD_SENTINEL = "\u0000__analytics_default_field__";

    /**
     * Relevance functions whose query string carries in-string {@code field:value} syntax and should
     * therefore be parsed for in-string fields. Only {@code query_string} qualifies today;
     * {@code simple_query_string}/{@code multi_match} have no in-string field grammar (a {@code :} is
     * treated literally), so their referenced fields come solely from the {@code fields} operand. Add
     * future functions here. Entries match the serializers' {@code functionName()}.
     */
    private static final Set<String> QUERY_STRING_FUNCTIONS = Set.of("query_string");

    private RelevanceFieldExtractor() {}

    /**
     * Computes the referenced fields for a relevance predicate.
     *
     * <p>Explicit {@code fields}/{@code field} tokens are collected for every relevance function; the
     * query string is additionally parsed for in-string fields only when {@code functionName} is a
     * query-string-style function (see {@link #QUERY_STRING_FUNCTIONS}).
     *
     * @param functionName   the relevance function name (e.g. {@code "query_string"}); decides whether
     *                       the query string is parsed for in-string fields, and used for log context
     * @param operands       the predicate's extracted operands (fields, query, etc.)
     * @param optionalParams the predicate's optional key/value params; the {@code lenient} flag is
     *                       read from here (absent → non-lenient, preserving eager rejection)
     */
    static FieldReferences referencedFields(
        String functionName,
        ConversionUtils.RelevanceOperands operands,
        Map<String, String> optionalParams
    ) {
        // Collect referenced field tokens in first-appearance order, deduped.
        Set<String> tokens = new LinkedHashSet<>();

        // 1. Explicit fields from the `fields`/`field` operand (already mapping-resolved for $ref forms).
        if (operands.fields() != null) {
            tokens.addAll(operands.fields());
        }
        if (operands.fieldName() != null) {
            tokens.add(operands.fieldName());
        }

        // 2. Only query_string carries in-string field:value syntax; parse it for in-string fields.
        if (QUERY_STRING_FUNCTIONS.contains(functionName) && operands.query() != null) {
            collectInStringFields(functionName, operands.query(), tokens);
        }

        // 3. Classify literal vs. pattern; only literals are validated by the planner.
        List<String> literalFields = new ArrayList<>();
        List<String> patternTokens = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                patternTokens.add(token);
            } else {
                literalFields.add(token);
            }
        }

        return new FieldReferences(literalFields, patternTokens, resolveLenient(optionalParams));
    }

    /**
     * Parses the query string with Lucene's classic grammar and records every field a leaf clause
     * touches. The sentinel default field marks fan-out; {@code _exists_:field} contributes the
     * named field (its term text) rather than {@code _exists_}.
     *
     * <p>Best-effort: the plan-time parser does not implement OpenSearch's {@code query_string}
     * extensions (e.g. {@code field:>15} unbounded ranges). On any parse failure, in-string
     * extraction is skipped and validation relies on the explicit {@code fields} operand — the
     * authoritative parse happens at execution. Unqualified terms resolve to the sentinel default
     * field and are not emitted as field references.
     */
    private static void collectInStringFields(String functionName, String queryString, Set<String> tokens) {
        QueryParser parser = new QueryParser(DEFAULT_FIELD_SENTINEL, Lucene.KEYWORD_ANALYZER);
        parser.setAllowLeadingWildcard(true);
        Query query;
        try {
            query = parser.parse(queryString);
        } catch (ParseException | RuntimeException e) {
            LOGGER.warn(
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
                // Skip the sentinel (unqualified terms) and _exists_ (handled via term text below).
                if (DEFAULT_FIELD_SENTINEL.equals(field) == false && EXISTS_FIELD.equals(field) == false) {
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
     * (assume non-lenient when unset). Treating unset as non-lenient preserves the planner's eager
     * rejection of explicitly-named non-text fields; an explicit {@code lenient=true} opts out.
     */
    private static boolean resolveLenient(Map<String, String> optionalParams) {
        String value = optionalParams.get("lenient");
        return value != null && Boolean.parseBoolean(value);
    }
}
