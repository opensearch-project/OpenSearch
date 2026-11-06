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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A {@link MappedFieldType} that has the same value for all documents.
 * Factory methods for queries are called at rewrite time so they should be
 * cheap. In particular they should not read data from disk or perform a
 * network call. Furthermore they may only return a {@link MatchAllDocsQuery}
 * or a {@link MatchNoDocsQuery}.
 *
 * @opensearch.internal
 */
public abstract class ConstantFieldType extends MappedFieldType {

    public ConstantFieldType(String name, Map<String, String> meta) {
        super(name, true, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    /**
     * Return whether the constant value of this field matches the provided {@code pattern}
     * as documented in {@link Regex#simpleMatch}.
     */
    protected abstract boolean matches(String pattern, boolean caseInsensitive, QueryShardContext context);

    static String valueToString(Object value) {
        return value instanceof BytesRef bytesRef ? bytesRef.utf8ToString() : value.toString();
    }

    @Override
    public final Query termQuery(Object value, QueryShardContext context) {
        String pattern = valueToString(value);
        if (matches(pattern, false, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    @Override
    public final Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
        String pattern = valueToString(value);
        if (matches(pattern, true, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    @Override
    public final Query termsQuery(List<?> values, QueryShardContext context) {
        for (Object value : values) {
            String pattern = valueToString(value);
            if (matches(pattern, false, context)) {
                // `terms` queries are a disjunction, so one matching term is enough
                return Queries.newMatchAllQuery();
            }
        }
        return new MatchNoDocsQuery();
    }

    @Override
    public final Query prefixQuery(
        String prefix,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        QueryShardContext context
    ) {
        String pattern = prefix + "*";
        if (matches(pattern, caseInsensitive, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    @Override
    public final Query wildcardQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitive,
        QueryShardContext context
    ) {
        if (matches(value, caseInsensitive, context)) {
            return Queries.newMatchAllQuery();
        } else {
            return new MatchNoDocsQuery();
        }
    }

    /**
     * Returns the value every document of the index has for this field, or {@code null} if this field type
     * does not support terms enumeration.
     */
    @Nullable
    protected String constantValue(QueryShardContext context) {
        return null;
    }

    /**
     * Returns the {@link #constantValue} as a single synthesized term when it starts with {@code prefix}, sorts
     * after {@code searchAfter} and {@code reader} has at least one live document. The prefix is matched
     * literally, with ASCII-only case folding when {@code caseInsensitive} is set.
     */
    @Override
    public TermsEnum termsEnum(
        IndexReader reader,
        String prefix,
        boolean caseInsensitive,
        @Nullable BytesRef searchAfter,
        QueryShardContext context
    ) throws IOException {
        String value = constantValue(context);
        if (value == null) {
            return super.termsEnum(reader, prefix, caseInsensitive, searchAfter, context);
        }
        // an early exit only: the collector still checks each segment's live docs before returning the term
        if (reader.numDocs() == 0) {
            return null;
        }
        boolean matches = caseInsensitive ? startsWithIgnoreAsciiCase(value, prefix) : value.startsWith(prefix);
        BytesRef term = new BytesRef(value);
        if (matches == false || (searchAfter != null && term.compareTo(searchAfter) <= 0)) {
            return null;
        }
        return new ConstantTermsEnum(term, reader.maxDoc());
    }

    /**
     * Whether {@code value} starts with {@code prefix}, comparing code point by code point and folding the case of ASCII
     * letters only; every other code point must be equal. This accepts exactly what the automaton built by
     * {@code AutomatonQueries.caseInsensitivePrefix(prefix)} accepts, without allocating anything.
     */
    private static boolean startsWithIgnoreAsciiCase(String value, String prefix) {
        int v = 0;
        for (int p = 0; p < prefix.length();) {
            if (v >= value.length()) {
                return false;
            }
            int expected = prefix.codePointAt(p);
            int actual = value.codePointAt(v);
            // like AutomatonQueries.toCaseInsensitiveChar, only code points up to 128 match their other case
            if (actual != expected && (expected > 128 || actual != otherCase(expected))) {
                return false;
            }
            p += Character.charCount(expected);
            v += Character.charCount(actual);
        }
        return true;
    }

    private static int otherCase(int codePoint) {
        return Character.isLowerCase(codePoint) ? Character.toUpperCase(codePoint) : Character.toLowerCase(codePoint);
    }
}
