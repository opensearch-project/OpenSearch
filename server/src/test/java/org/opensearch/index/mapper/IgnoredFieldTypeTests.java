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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.OpenSearchException;

public class IgnoredFieldTypeTests extends FieldTypeTestCase {

    public void testPrefixQuery() {
        MappedFieldType ft = IgnoredFieldMapper.IgnoredFieldType.INSTANCE;

        Query expected = new PrefixQuery(new Term("_ignored", new BytesRef("foo*")), MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);
        assertEquals(expected, ft.prefixQuery("foo*", null, MOCK_QSC));

        OpenSearchException ee = expectThrows(OpenSearchException.class, () -> ft.prefixQuery("foo*", null, MOCK_QSC_DISALLOW_EXPENSIVE));
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = IgnoredFieldMapper.IgnoredFieldType.INSTANCE;

        Query expected = new RegexpQuery(
            new Term("_ignored", new BytesRef("foo?")),
            RegExp.ALL,
            0,
            RegexpQuery.DEFAULT_PROVIDER,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE
        );
        assertEquals(expected, ft.regexpQuery("foo?", 0, 0, 10, null, MOCK_QSC));

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.regexpQuery("foo?", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildcardQuery() {
        MappedFieldType ft = IgnoredFieldMapper.IgnoredFieldType.INSTANCE;

        Query expected = new WildcardQuery(new Term("_ignored", new BytesRef("foo*")));
        assertEquals(expected, ft.wildcardQuery("foo*", MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC));

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.wildcardQuery("valu*", null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }
}
