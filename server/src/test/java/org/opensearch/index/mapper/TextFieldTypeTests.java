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
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.mapper.TextFieldMapper.TextFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;

public class TextFieldTypeTests extends FieldTypeTestCase {

    TextFieldType createFieldType(boolean searchabe) {
        if (searchabe) {
            return new TextFieldType("field");
        } else {
            return new TextFieldType("field", false, false, Collections.emptyMap());
        }
    }

    public void testIsAggregatableDependsOnFieldData() {
        TextFieldType ft = createFieldType(true);
        assertFalse(ft.isAggregatable());
        ft.setFielddata(true);
        assertTrue(ft.isAggregatable());
    }

    public void testTermQuery() {
        MappedFieldType ft = createFieldType(true);
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", null));
        assertEquals(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo")), ft.termQueryCaseInsensitive("fOo", null));

        MappedFieldType unsearchable = createFieldType(false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermsQuery() {
        MappedFieldType ft = createFieldType(true);
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        assertEquals(new TermInSetQuery("field", terms), ft.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType unsearchable = createFieldType(false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), null)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testRangeQuery() {
        MappedFieldType ft = createFieldType(true);
        assertEquals(
            new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false),
            ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC)
        );

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[range] queries on [text] or [keyword] fields cannot be executed when " + "'search.allow_expensive_queries' is set to false.",
            ee.getMessage()
        );
    }

    public void testRegexpQuery() {
        MappedFieldType ft = createFieldType(true);
        assertEquals(
            new RegexpQuery(new Term("field", "foo.*")),
            ft.regexpQuery("foo.*", 0, 0, 10, CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC)
        );

        MappedFieldType unsearchable = createFieldType(false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.regexpQuery("foo.*", 0, 0, 10, null, MOCK_QSC)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = createFieldType(true);
        assertEquals(
            new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC)
        );

        MappedFieldType unsearchable = createFieldType(false);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC)
        );
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.fuzzyQuery("foo", Fuzziness.AUTO, randomInt(10) + 1, randomInt(10) + 1, randomBoolean(), MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testIndexPrefixes() {
        TextFieldType ft = createFieldType(true);
        ft.setIndexAnalyzer(Lucene.STANDARD_ANALYZER);
        ft.setPrefixFieldType(new TextFieldMapper.PrefixFieldType(ft, "field._index_prefix", 2, 10));

        Query q = ft.prefixQuery("goin", CONSTANT_SCORE_REWRITE, false, randomMockShardContext());
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field._index_prefix", "goin"))), q);

        q = ft.prefixQuery("internationalisatio", CONSTANT_SCORE_REWRITE, false, MOCK_QSC);
        assertEquals(new PrefixQuery(new Term("field", "internationalisatio"), CONSTANT_SCORE_REWRITE), q);

        q = ft.prefixQuery("Internationalisatio", CONSTANT_SCORE_REWRITE, true, MOCK_QSC);
        assertEquals(AutomatonQueries.caseInsensitivePrefixQuery(new Term("field", "Internationalisatio")), q);

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.prefixQuery("internationalisatio", null, false, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals(
            "[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false. "
                + "For optimised prefix queries on text fields please enable [index_prefixes].",
            ee.getMessage()
        );

        q = ft.prefixQuery("g", CONSTANT_SCORE_REWRITE, false, randomMockShardContext());
        Automaton automaton = Operations.concatenate(Arrays.asList(Automata.makeChar('g'), Automata.makeAnyChar()));

        Query expected = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(
                new AutomatonQuery(
                    new Term("field._index_prefix", "g*"),
                    automaton,
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    false,
                    CONSTANT_SCORE_REWRITE
                ),
                BooleanClause.Occur.SHOULD
            ).add(new TermQuery(new Term("field", "g")), BooleanClause.Occur.SHOULD).build()
        );

        assertThat(q, equalTo(expected));

        q = ft.prefixQuery("g", null, false, randomMockShardContext());
        automaton = Operations.concatenate(Arrays.asList(Automata.makeChar('g'), Automata.makeAnyChar()));

        expected = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(
                new AutomatonQuery(
                    new Term("field._index_prefix", "g*"),
                    automaton,
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    false,
                    CONSTANT_SCORE_REWRITE
                ),
                BooleanClause.Occur.SHOULD
            ).add(new TermQuery(new Term("field", "g")), BooleanClause.Occur.SHOULD).build()
        );

        assertThat(q, equalTo(expected));
    }

    public void testFetchSourceValue() throws IOException {
        TextFieldType fieldType = createFieldType(true);
        fieldType.setIndexAnalyzer(Lucene.STANDARD_ANALYZER);

        assertEquals(List.of("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(fieldType, true));

        TextFieldMapper.PrefixFieldType prefixFieldType = new TextFieldMapper.PrefixFieldType(fieldType, "field._index_prefix", 2, 10);
        assertEquals(List.of("value"), fetchSourceValue(prefixFieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(prefixFieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(prefixFieldType, true));

        TextFieldMapper.PhraseFieldType phraseFieldType = new TextFieldMapper.PhraseFieldType(fieldType);
        assertEquals(List.of("value"), fetchSourceValue(phraseFieldType, "value"));
        assertEquals(List.of("42"), fetchSourceValue(phraseFieldType, 42L));
        assertEquals(List.of("true"), fetchSourceValue(phraseFieldType, true));
    }
}
