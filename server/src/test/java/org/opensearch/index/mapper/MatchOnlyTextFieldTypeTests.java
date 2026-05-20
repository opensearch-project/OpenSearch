/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.TermQuery;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;

public class MatchOnlyTextFieldTypeTests extends TextFieldTypeTests {

    @Override
    TextFieldMapper.TextFieldType createFieldType(boolean searchable) {
        TextSearchInfo tsi = new TextSearchInfo(
            TextFieldMapper.Defaults.FIELD_TYPE,
            null,
            Lucene.STANDARD_ANALYZER,
            Lucene.STANDARD_ANALYZER
        );
        return new MatchOnlyTextFieldMapper.MatchOnlyTextFieldType(
            "field",
            searchable,
            false,
            tsi,
            ParametrizedFieldMapper.Parameter.metaParam().get()
        );
    }

    @Override
    public void testTermQuery() {
        MappedFieldType ft = createFieldType(true);
        assertEquals(new ConstantScoreQuery(new TermQuery(new Term("field", "foo"))), ft.termQuery("foo", null));
        assertEquals(
            new ConstantScoreQuery(AutomatonQueries.caseInsensitiveTermQuery(new Term("field", "fOo"))),
            ft.termQueryCaseInsensitive("fOo", null)
        );

        MappedFieldType unsearchable = createFieldType(false);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }
}
