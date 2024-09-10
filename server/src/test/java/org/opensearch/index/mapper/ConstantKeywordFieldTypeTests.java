/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.QueryShardContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.lucene.search.MultiTermQuery.CONSTANT_SCORE_REWRITE;

public class ConstantKeywordFieldTypeTests extends FieldTypeTestCase {

    // encode: abcde->0, fgh->1, xYy->2, xyy->3
    final String[] constant_keywords = new String[] { "abcde", "fgh", "xYy", "xyz" };

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType oneValueFT = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
        "field",
        new String[] { "default" },
        true,
        true
    );

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType multiValuesFT = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
        "field",
        constant_keywords,
        true,
        true
    );

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType multiValuesOnlyIndex =
        new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", constant_keywords, false, true);

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType multiValuesOnlyDocValues =
        new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", constant_keywords, true, false);

    final ConstantKeywordFieldMapper.ConstantKeywordFieldType multiValuesNoneDocValueNoIndex =
        new ConstantKeywordFieldMapper.ConstantKeywordFieldType("field", constant_keywords, false, false);

    public void testTermQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.termQuery("default", createContext()));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.termQuery("not_default", createContext()));

        assertEquals(new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))), multiValuesFT.termQuery("fgh", null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.termQuery("fgh1", null));

        assertEquals(new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))), multiValuesOnlyIndex.termQuery("fgh", null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.termQuery("fgh1", null));

        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", new BytesRef(new byte[] { 1 })),
            multiValuesOnlyDocValues.termQuery("fgh", null)
        );

        Exception e = expectThrows(IllegalArgumentException.class, () -> multiValuesNoneDocValueNoIndex.termQuery("fgh", null));
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testInsensitiveTermQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.termQueryCaseInsensitive("defaUlt", createContext()));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.termQueryCaseInsensitive("not_defaUlt", createContext()));

        assertEquals(new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))), multiValuesFT.termQueryCaseInsensitive("fGh", null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.termQueryCaseInsensitive("fgh1", null));

        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))),
            multiValuesOnlyIndex.termQueryCaseInsensitive("fGh", null)
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyIndex.termQueryCaseInsensitive("fgh1", null));
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))),
            multiValuesOnlyIndex.termQueryCaseInsensitive("fGh", null)
        );

        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.termQueryCaseInsensitive("fGh", null)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testTermsQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.termsQuery(Arrays.asList("default", "not_default"), createContext()));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.termsQuery(Arrays.asList("no_default", "not_default"), createContext()));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.termsQuery(List.of(), createContext()));

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 3 }));
        terms.add(new BytesRef(new byte[] { 1 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );
        assertEquals(expected, multiValuesFT.termsQuery(List.of("xyz", "fgh", "xy2"), null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.termsQuery(List.of("fgh1"), null));

        assertEquals(new TermInSetQuery("field", terms), multiValuesOnlyIndex.termsQuery(List.of("xyz", "fgh", "xy2"), null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.termQueryCaseInsensitive("fgh1", null));

        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.termsQuery(List.of("xyz", "fgh", "xy2"), null)
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyDocValues.termQueryCaseInsensitive("fgh1", null));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.termsQuery(List.of("xyz", "fgh", "xy2"), null)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testPrefixQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.prefixQuery("defau", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), oneValueFT.prefixQuery("deFau", null, true, createContext()));

        assertEquals(new MatchNoDocsQuery(), oneValueFT.prefixQuery("not_default", null, createContext()));

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 2 }));
        terms.add(new BytesRef(new byte[] { 3 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );

        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 2 }))),
            multiValuesFT.prefixQuery("xY", CONSTANT_SCORE_REWRITE, false, null)
        );
        assertEquals(expected, multiValuesFT.prefixQuery("xY", CONSTANT_SCORE_REWRITE, true, null));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.prefixQuery("xY1", CONSTANT_SCORE_REWRITE, true, null));

        assertEquals(new TermInSetQuery("field", terms), multiValuesOnlyIndex.prefixQuery("xy", CONSTANT_SCORE_REWRITE, true, null));
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 2 }))),
            multiValuesOnlyIndex.prefixQuery("xY", CONSTANT_SCORE_REWRITE, false, null)
        );

        assertEquals(new TermInSetQuery("field", terms), multiValuesOnlyDocValues.prefixQuery("xY", CONSTANT_SCORE_REWRITE, true, null));
        terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 2 }));
        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.prefixQuery("xY", CONSTANT_SCORE_REWRITE, false, null)
        );

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.termQueryCaseInsensitive("fGh", null)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testFuzzyQuery() {

        assertEquals(new MatchAllDocsQuery(), oneValueFT.fuzzyQuery("defauab", Fuzziness.fromEdits(2), 0, 1, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.fuzzyQuery("defauab", Fuzziness.fromEdits(1), 0, 1, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 3 }));
        terms.add(new BytesRef(new byte[] { 2 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );

        assertEquals(expected, multiValuesFT.fuzzyQuery("xz", Fuzziness.build("2"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 3 }))),
            multiValuesFT.fuzzyQuery("xz", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.fuzzyQuery("x11", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        assertEquals(
            new TermInSetQuery("field", terms),
            multiValuesOnlyIndex.fuzzyQuery("xz", Fuzziness.build("2"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 3 }))),
            multiValuesOnlyIndex.fuzzyQuery("xz", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyIndex.fuzzyQuery("x11", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.fuzzyQuery("xz", Fuzziness.build("2"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", new BytesRef(new byte[] { 3 })),
            multiValuesOnlyDocValues.fuzzyQuery("xz", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyDocValues.fuzzyQuery("x11", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.fuzzyQuery("xz", Fuzziness.build("1"), 0, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testWildcardQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.wildcardQuery("defa*lt", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), oneValueFT.wildcardQuery("no_defa*lt", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), oneValueFT.wildcardQuery("defa*", null, createContext()));
        assertEquals(new MatchAllDocsQuery(), oneValueFT.wildcardQuery("*ult", null, createContext()));

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 3 }));
        terms.add(new BytesRef(new byte[] { 2 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );
        assertEquals(expected, multiValuesFT.wildcardQuery("x*y*", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), multiValuesFT.wildcardQuery("*k*y*", null, createContext()));

        assertEquals(new TermInSetQuery("field", terms), multiValuesOnlyIndex.wildcardQuery("x*y*", null, createContext()));
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyIndex.wildcardQuery("*k*y*", null, createContext()));

        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.wildcardQuery("x*y*", null, createContext())
        );
        assertEquals(new MatchNoDocsQuery(), multiValuesOnlyDocValues.wildcardQuery("*k*y*", null, createContext()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.wildcardQuery("x*y*", null, createContext())
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());
    }

    public void testExistsQuery() {
        assertEquals(new MatchAllDocsQuery(), oneValueFT.existsQuery(createContext()));

        assertEquals(new DocValuesFieldExistsQuery("field"), multiValuesFT.existsQuery(createContext()));

        assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), multiValuesOnlyIndex.existsQuery(createContext()));

        assertEquals(new DocValuesFieldExistsQuery("field"), multiValuesOnlyDocValues.existsQuery(createContext()));

        assertEquals(new MatchNoDocsQuery(), multiValuesNoneDocValueNoIndex.existsQuery(createContext()));
    }

    public void testRangeQuery() {
        Query actual = oneValueFT.rangeQuery("default", null, true, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = oneValueFT.rangeQuery("default", null, false, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = oneValueFT.rangeQuery(null, "default", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = oneValueFT.rangeQuery(null, "default", false, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = oneValueFT.rangeQuery("default", "default", false, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = oneValueFT.rangeQuery("default", "default", true, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), actual);

        actual = oneValueFT.rangeQuery(null, null, false, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = oneValueFT.rangeQuery("default", "default", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), actual);

        actual = oneValueFT.rangeQuery("defaul", "default1", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), actual);

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 1 }));
        terms.add(new BytesRef(new byte[] { 2 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );
        assertEquals(expected, multiValuesFT.rangeQuery("f", "xYz", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))),
            multiValuesFT.rangeQuery("f", "xYy", true, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(new DocValuesFieldExistsQuery("field"), multiValuesFT.rangeQuery("a", "z", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        assertEquals(
            new TermInSetQuery("field", terms),
            multiValuesOnlyIndex.rangeQuery("f", "xYz", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermQuery(new Term("field", new BytesRef(new byte[] { 1 }))),
            multiValuesOnlyIndex.rangeQuery("f", "xYy", true, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")),
            multiValuesOnlyIndex.rangeQuery("a", "z", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );

        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.rangeQuery("abcde", "xyz", false, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", new BytesRef(new byte[] { 1 })),
            multiValuesOnlyDocValues.rangeQuery("abcde", "xYy", false, false, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new DocValuesFieldExistsQuery("field"),
            multiValuesOnlyDocValues.rangeQuery("a", "z", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );

        assertEquals(new MatchNoDocsQuery(), multiValuesNoneDocValueNoIndex.rangeQuery("a", "z", true, true, null, null, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));
    }

    public void testRegexpQuery() {
        final ConstantKeywordFieldMapper.ConstantKeywordFieldType oneValueFT = new ConstantKeywordFieldMapper.ConstantKeywordFieldType(
            "field",
            new String[] { "d3efault" },
            true,
            true
        );
        // test .*
        Query query = oneValueFT.regexpQuery("d.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), query);
        // test \d and ?
        query = oneValueFT.regexpQuery("d\\defau[a-z]?t", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), query);

        // test \d and ?
        query = oneValueFT.regexpQuery("d\\defa[a-z]?t", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), query);
        // \w{m,n}
        query = oneValueFT.regexpQuery("d3efa[a-z]{3,3}", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchAllDocsQuery(), query);
        // \w{m,n}
        query = oneValueFT.regexpQuery("d3efa[a-z]{4,4}", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES);
        assertEquals(new MatchNoDocsQuery(), query);

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef(new byte[] { 2 }));
        terms.add(new BytesRef(new byte[] { 3 }));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );
        assertEquals(expected, multiValuesFT.regexpQuery("x.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        assertEquals(
            new TermInSetQuery("field", terms),
            multiValuesOnlyIndex.regexpQuery("x.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals(
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms),
            multiValuesOnlyDocValues.regexpQuery("x.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> multiValuesNoneDocValueNoIndex.regexpQuery("x.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );
        assertEquals("Cannot search on field [field] since it is both not indexed, and does not have doc_values enabled.", e.getMessage());

    }

    private QueryShardContext createContext() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        Predicate<String> indexNameMatcher = pattern -> Regex.simpleMatch(pattern, "index");
        return new QueryShardContext(
            0,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            writableRegistry(),
            null,
            null,
            System::currentTimeMillis,
            null,
            indexNameMatcher,
            () -> true,
            null
        );
    }
}
