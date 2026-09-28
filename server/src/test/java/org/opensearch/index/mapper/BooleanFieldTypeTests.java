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

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BooleanFieldTypeTests extends FieldTypeTestCase {

    /** Pluggable-dataformat context — for tests that exercise the isEffectiveSearchable gate. */
    private static QueryShardContext pluggableContext() {
        QueryShardContext ctx = mock(QueryShardContext.class);
        when(ctx.isPluggableDataFormatEnabled()).thenReturn(true);
        return ctx;
    }

    public void testValueFormat() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(false, ft.docValueFormat(null, null).format(0));
        assertEquals(true, ft.docValueFormat(null, null).format(1));
    }

    public void testValueForSearch() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(true, ft.valueForDisplay("T"));
        assertEquals(false, ft.valueForDisplay("F"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay(0));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("true"));
        expectThrows(IllegalArgumentException.class, () -> ft.valueForDisplay("G"));
    }

    public void testTermQuery() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(new TermQuery(new Term("field", "T")), ft.termQuery("true", MOCK_QSC));
        assertEquals(new TermQuery(new Term("field", "F")), ft.termQuery("false", MOCK_QSC));

        MappedFieldType doc_ft = new BooleanFieldMapper.BooleanFieldType("field", false, true);
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 1), doc_ft.termQuery("true", MOCK_QSC));
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 0), doc_ft.termQuery("false", MOCK_QSC));

        MappedFieldType boost_ft = new BooleanFieldMapper.BooleanFieldType("field");
        boost_ft.setBoost(2f);
        assertEquals(new BoostQuery(new TermQuery(new Term("field", "T")), 2f), boost_ft.termQuery("true", MOCK_QSC));
        assertEquals(new BoostQuery(new TermQuery(new Term("field", "F")), 2f), boost_ft.termQuery("false", MOCK_QSC));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("true", MOCK_QSC));
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    /**
     * On a pluggable-dataformat index the boolean mapper must route to the doc-values exact
     * query rather than a postings {@code TermQuery}. The Lucene secondary writes no postings
     * for boolean fields on such indices, so a {@code TermQuery} would return zero hits.
     */
    public void testTermQueryUsesDocValuesWhenPluggableDataFormatEnabled() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        QueryShardContext pluggable = pluggableContext();
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 1), ft.termQuery("true", pluggable));
        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 0), ft.termQuery("false", pluggable));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("true"));
        terms.add(new BytesRef("false"));
        assertEquals(new FieldExistsQuery("field"), ft.termsQuery(terms, MOCK_QSC));

        List<BytesRef> newTerms = new ArrayList<>();
        newTerms.add(new BytesRef("true"));
        assertEquals(new TermQuery(new Term("field", "T")), ft.termsQuery(newTerms, MOCK_QSC));

        List<BytesRef> incorrectTerms = new ArrayList<>();
        incorrectTerms.add(new BytesRef("true"));
        incorrectTerms.add(new BytesRef("random"));
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> ft.termsQuery(incorrectTerms, MOCK_QSC));
        assertEquals("Can't parse boolean value [random], expected [true] or [false]", ex.getMessage());

        MappedFieldType doc_only_ft = new BooleanFieldMapper.BooleanFieldType("field", false, true);

        assertEquals(SortedNumericDocValuesField.newSlowExactQuery("field", 1), doc_only_ft.termsQuery(newTerms, MOCK_QSC));

        MappedFieldType unsearchable = new BooleanFieldMapper.BooleanFieldType("field", false, false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termsQuery(terms, MOCK_QSC));
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testRangeQuery() {
        BooleanFieldMapper.BooleanFieldType ft = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(new FieldExistsQuery("field"), ft.rangeQuery(false, true, true, true, MOCK_QSC));

        assertEquals(new TermQuery(new Term("field", "T")), ft.rangeQuery(false, true, false, true, MOCK_QSC));

        assertEquals(new TermQuery(new Term("field", "F")), ft.rangeQuery(false, true, true, false, MOCK_QSC));

        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(false, true, false, false, MOCK_QSC));

        assertEquals(new MatchNoDocsQuery(), ft.rangeQuery(false, true, false, false, MOCK_QSC));

        assertEquals(new TermQuery(new Term("field", "F")), ft.rangeQuery(false, false, true, true, MOCK_QSC));

        assertEquals(new TermQuery(new Term("field", "F")), ft.rangeQuery(null, false, true, true, MOCK_QSC));

        assertEquals(new FieldExistsQuery("field"), ft.rangeQuery(false, null, true, true, MOCK_QSC));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ft.rangeQuery("random", null, true, true, MOCK_QSC)
        );

        assertEquals("Can't parse boolean value [random], expected [true] or [false]", e.getMessage());
    }

    public void testFetchSourceValue() throws IOException {

        MappedFieldType fieldType = new BooleanFieldMapper.BooleanFieldType("field");
        assertEquals(Collections.singletonList(true), fetchSourceValue(fieldType, true));
        assertEquals(Collections.singletonList(false), fetchSourceValue(fieldType, "false"));
        assertEquals(Collections.singletonList(false), fetchSourceValue(fieldType, ""));

        MappedFieldType nullFieldType = new BooleanFieldMapper.BooleanFieldType("field", true, false, true, true, Collections.emptyMap());
        assertEquals(Collections.singletonList(true), fetchSourceValue(nullFieldType, null));
    }
}
