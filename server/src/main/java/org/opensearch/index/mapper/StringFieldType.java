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

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.QueryShardContext;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.apache.lucene.search.FuzzyQuery.defaultRewriteMethod;

/** Base class for {@link MappedFieldType} implementations that use the same
 * representation for internal index terms as the external representation so
 * that partial matching queries such as prefix, wildcard and fuzzy queries
 * can be implemented.
 *
 * @opensearch.internal
 */
public abstract class StringFieldType extends TermBasedFieldType {

    private static final Pattern WILDCARD_PATTERN = Pattern.compile("(\\\\.)|([?*]+)");

    public StringFieldType(
        String name,
        boolean isSearchable,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        super(name, isSearchable, isStored, hasDocValues, textSearchInfo, meta);
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
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[fuzzy] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        failIfNotIndexed();
        return new FuzzyQuery(
            new Term(name(), indexedValueForSearch(value)),
            fuzziness.asDistance(BytesRefs.toString(value)),
            prefixLength,
            maxExpansions,
            transpositions
        );
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
        if (!context.allowExpensiveQueries()) {
            throw new OpenSearchException(
                "[fuzzy] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        failIfNotIndexed();
        if (method == null) {
            method = defaultRewriteMethod(maxExpansions);
        }
        return new FuzzyQuery(
            new Term(name(), indexedValueForSearch(value)),
            fuzziness.asDistance(BytesRefs.toString(value)),
            prefixLength,
            maxExpansions,
            transpositions,
            method
        );
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[prefix] queries cannot be executed when '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false. For optimised prefix queries on text "
                    + "fields please enable [index_prefixes]."
            );
        }
        failIfNotIndexed();
        if (method == null) {
            method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
        }
        if (caseInsensitive) {
            return AutomatonQueries.caseInsensitivePrefixQuery((new Term(name(), indexedValueForSearch(value))), method);
        }
        return new PrefixQuery(new Term(name(), indexedValueForSearch(value)), method);
    }

    public static final String normalizeWildcardPattern(String fieldname, String value, Analyzer normalizer) {
        if (normalizer == null) {
            return value;
        }
        // we want to normalize everything except wildcard characters, e.g. F?o Ba* to f?o ba*, even if e.g there
        // is a char_filter that would otherwise remove them
        Matcher wildcardMatcher = WILDCARD_PATTERN.matcher(value);
        BytesRefBuilder sb = new BytesRefBuilder();
        int last = 0;

        while (wildcardMatcher.find()) {
            if (wildcardMatcher.start() > 0) {
                String chunk = value.substring(last, wildcardMatcher.start());

                BytesRef normalized = normalizer.normalize(fieldname, chunk);
                sb.append(normalized);
            }
            // append the matched group - without normalizing
            sb.append(new BytesRef(wildcardMatcher.group()));

            last = wildcardMatcher.end();
        }
        if (last < value.length()) {
            String chunk = value.substring(last);
            BytesRef normalized = normalizer.normalize(fieldname, chunk);
            sb.append(normalized);
        }
        return sb.toBytesRef().utf8ToString();
    }

    /** optionally normalize the wildcard pattern based on the value of {@code caseInsensitive} */
    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
        return wildcardQuery(value, method, caseInsensitive, false, context);
    }

    /** always normalizes the wildcard pattern to lowercase */
    @Override
    public Query normalizedWildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        return wildcardQuery(value, method, false, true, context);
    }

    /**
     * return a wildcard query
     *
     * @param value the pattern
     * @param method rewrite method
     * @param caseInsensitive should ignore case; note, only used if there is no analyzer, else we use the analyzer rules
     * @param normalizeIfAnalyzed force normalize casing if an analyzer is used
     * @param context the query shard context
     */
    public Query wildcardQuery(
        String value,
        MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        boolean normalizeIfAnalyzed,
        QueryShardContext context
    ) {
        failIfNotIndexed();
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[wildcard] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        Term term;
        if (getTextSearchInfo().getSearchAnalyzer() != null && normalizeIfAnalyzed) {
            value = normalizeWildcardPattern(name(), value, getTextSearchInfo().getSearchAnalyzer());
            term = new Term(name(), value);
        } else {
            term = new Term(name(), indexedValueForSearch(value));
        }
        if (caseInsensitive) {
            return AutomatonQueries.caseInsensitiveWildcardQuery(term, method);
        }
        if (method == null) {
            method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
        }
        return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, method);
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
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        failIfNotIndexed();
        if (method == null) {
            method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
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
    public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[range] queries on [text] or [keyword] fields cannot be executed when '"
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "' is set to false."
            );
        }
        failIfNotIndexed();
        return new TermRangeQuery(
            name(),
            lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
            upperTerm == null ? null : indexedValueForSearch(upperTerm),
            includeLower,
            includeUpper
        );
    }
}
