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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.CustomAnalyzer;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.mapper.MappedFieldType.Relation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KeywordFieldTypeTests extends FieldTypeTestCase {

    public void testIsFieldWithinQuery() throws IOException {
        KeywordFieldType ft = new KeywordFieldType("field");
        // current impl ignores args and should always return INTERSECTS
        assertEquals(
            Relation.INTERSECTS,
            ft.isFieldWithinQuery(
                null,
                RandomStrings.randomAsciiLettersOfLengthBetween(random(), 0, 5),
                RandomStrings.randomAsciiLettersOfLengthBetween(random(), 0, 5),
                randomBoolean(),
                randomBoolean(),
                null,
                null,
                null
            )
        );
    }

    public void testTermQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", "foo")), ft.termQuery("foo", null));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, true, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> unsearchable.termQuery("bar", null));
        assertEquals("Cannot search on field [field] since it is not indexed.", e.getMessage());
    }

    public void testTermQueryWithNormalizer() {
        Analyzer normalizer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer in = new WhitespaceTokenizer();
                TokenFilter out = new LowerCaseFilter(in);
                return new TokenStreamComponents(in, out);
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return new LowerCaseFilter(in);
            }
        };
        MappedFieldType ft = new KeywordFieldType("field", new NamedAnalyzer("my_normalizer", AnalyzerScope.INDEX, normalizer));
        assertEquals(new TermQuery(new Term("field", "foo bar")), ft.termQuery("fOo BaR", null));
    }

    public void testTermsQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("foo"));
        terms.add(new BytesRef("bar"));
        Query expected = new IndexOrDocValuesQuery(
            new TermInSetQuery("field", terms),
            new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms)
        );
        assertEquals(expected, ft.termsQuery(Arrays.asList("foo", "bar"), MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        MappedFieldType onlyIndexed = new KeywordFieldType("field", true, false, Collections.emptyMap());
        Query expectedIndex = new TermInSetQuery("field", terms);
        assertEquals(expectedIndex, onlyIndexed.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType onlyDocValues = new KeywordFieldType("field", false, true, Collections.emptyMap());
        Query expectedDocValues = new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, "field", terms);
        assertEquals(expectedDocValues, onlyDocValues.termsQuery(Arrays.asList("foo", "bar"), null));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.termsQuery(Arrays.asList("foo", "bar"), null)
        );
        assertEquals(
            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
            e.getMessage()
        );
    }

    public void testExistsQuery() {
        {
            KeywordFieldType ft = new KeywordFieldType("field");
            assertEquals(new DocValuesFieldExistsQuery("field"), ft.existsQuery(null));
        }
        {
            FieldType fieldType = new FieldType();
            fieldType.setOmitNorms(false);
            KeywordFieldType ft = new KeywordFieldType("field", fieldType);
            assertEquals(new NormsFieldExistsQuery("field"), ft.existsQuery(null));
        }
        {
            KeywordFieldType ft = new KeywordFieldType("field", true, false, Collections.emptyMap());
            assertEquals(new TermQuery(new Term(FieldNamesFieldMapper.NAME, "field")), ft.existsQuery(null));
        }
    }

    public void testRangeQuery() {
        MappedFieldType ft = new KeywordFieldType("field");

        Query indexExpected = new TermRangeQuery("field", BytesRefs.toBytesRef("foo"), BytesRefs.toBytesRef("bar"), true, false);
        Query dvExpected = new TermRangeQuery(
            "field",
            BytesRefs.toBytesRef("foo"),
            BytesRefs.toBytesRef("bar"),
            true,
            false,
            MultiTermQuery.DOC_VALUES_REWRITE
        );

        Query expected = new IndexOrDocValuesQuery(indexExpected, dvExpected);
        Query actual = ft.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC);
        assertEquals(expected, actual);

        MappedFieldType onlyIndexed = new KeywordFieldType("field", true, false, Collections.emptyMap());
        assertEquals(indexExpected, onlyIndexed.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC));

        MappedFieldType onlyDocValues = new KeywordFieldType("field", false, true, Collections.emptyMap());
        assertEquals(dvExpected, onlyDocValues.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.rangeQuery("foo", "bar", true, false, null, null, null, MOCK_QSC)
        );

        assertEquals(
            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
            e.getMessage()
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
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(
            new IndexOrDocValuesQuery(
                new RegexpQuery(new Term("field", "foo.*")),
                new RegexpQuery(new Term("field", "foo.*"), 0, 0, RegexpQuery.DEFAULT_PROVIDER, 10, MultiTermQuery.DOC_VALUES_REWRITE)
            ),
            ft.regexpQuery("foo.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );

        Query indexExpected = new RegexpQuery(new Term("field", "foo.*"));
        MappedFieldType onlyIndexed = new KeywordFieldType("field", true, false, Collections.emptyMap());
        assertEquals(indexExpected, onlyIndexed.regexpQuery("foo.*", 0, 0, 10, MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC));

        Query dvExpected = new RegexpQuery(
            new Term("field", "foo.*"),
            0,
            0,
            RegexpQuery.DEFAULT_PROVIDER,
            10,
            MultiTermQuery.DOC_VALUES_REWRITE
        );
        MappedFieldType onlyDocValues = new KeywordFieldType("field", false, true, Collections.emptyMap());
        assertEquals(dvExpected, onlyDocValues.regexpQuery("foo.*", 0, 0, 10, MultiTermQuery.DOC_VALUES_REWRITE, MOCK_QSC));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.regexpQuery("foo.*", 0, 0, 10, null, MOCK_QSC)
        );
        assertEquals(
            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
            e.getMessage()
        );

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.regexpQuery("foo.*", randomInt(10), 0, randomInt(10) + 1, null, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[regexp] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testFuzzyQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(
            new IndexOrDocValuesQuery(
                new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true),
                new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true, MultiTermQuery.DOC_VALUES_REWRITE)
            ),
            ft.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, null, MOCK_QSC_ENABLE_INDEX_DOC_VALUES)
        );

        Query indexExpected = new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true);
        MappedFieldType onlyIndexed = new KeywordFieldType("field", true, false, Collections.emptyMap());
        assertEquals(indexExpected, onlyIndexed.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MOCK_QSC));

        Query dvExpected = new FuzzyQuery(new Term("field", "foo"), 2, 1, 50, true, MultiTermQuery.DOC_VALUES_REWRITE);
        MappedFieldType onlyDocValues = new KeywordFieldType("field", false, true, Collections.emptyMap());
        assertEquals(
            dvExpected,
            onlyDocValues.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MultiTermQuery.DOC_VALUES_REWRITE, MOCK_QSC)
        );

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.fuzzyQuery("foo", Fuzziness.fromEdits(2), 1, 50, true, MultiTermQuery.DOC_VALUES_REWRITE, MOCK_QSC)
        );
        assertEquals(
            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
            e.getMessage()
        );

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.fuzzyQuery("foo", Fuzziness.AUTO, randomInt(10) + 1, randomInt(10) + 1, randomBoolean(), MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[fuzzy] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testWildCardQuery() {
        MappedFieldType ft = new KeywordFieldType("field");
        Query expected = new IndexOrDocValuesQuery(
            new WildcardQuery(new Term("field", new BytesRef("foo*"))),
            new WildcardQuery(
                new Term("field", new BytesRef("foo*")),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.DOC_VALUES_REWRITE
            )
        );
        assertEquals(expected, ft.wildcardQuery("foo*", MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_ENABLE_INDEX_DOC_VALUES));

        Query indexExpected = new WildcardQuery(new Term("field", new BytesRef("foo*")));
        MappedFieldType onlyIndexed = new KeywordFieldType("field", true, false, Collections.emptyMap());
        assertEquals(indexExpected, onlyIndexed.wildcardQuery("foo*", MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC));

        Query dvExpected = new WildcardQuery(
            new Term("field", new BytesRef("foo*")),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            MultiTermQuery.DOC_VALUES_REWRITE
        );
        MappedFieldType onlyDocValues = new KeywordFieldType("field", false, true, Collections.emptyMap());
        assertEquals(dvExpected, onlyDocValues.wildcardQuery("foo*", MultiTermQuery.DOC_VALUES_REWRITE, MOCK_QSC));

        MappedFieldType unsearchable = new KeywordFieldType("field", false, false, Collections.emptyMap());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> unsearchable.wildcardQuery("foo*", MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC)
        );
        assertEquals(
            "Cannot search on field [field] since it is both not indexed, and does not have doc_values " + "enabled.",
            e.getMessage()
        );

        OpenSearchException ee = expectThrows(
            OpenSearchException.class,
            () -> ft.wildcardQuery("foo*", MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE, MOCK_QSC_DISALLOW_EXPENSIVE)
        );
        assertEquals("[wildcard] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", ee.getMessage());
    }

    public void testNormalizeQueries() {
        MappedFieldType ft = new KeywordFieldType("field");
        assertEquals(new TermQuery(new Term("field", new BytesRef("FOO"))), ft.termQuery("FOO", null));
        ft = new KeywordFieldType("field", Lucene.STANDARD_ANALYZER);
        assertEquals(new TermQuery(new Term("field", new BytesRef("foo"))), ft.termQuery("FOO", null));
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new KeywordFieldMapper.Builder("field").build(context).fieldType();
        assertEquals(Collections.singletonList("value"), fetchSourceValue(mapper, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(mapper, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(mapper, true));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fetchSourceValue(mapper, "value", "format"));
        assertEquals("Field [field] of type [keyword] doesn't support formats.", e.getMessage());

        MappedFieldType ignoreAboveMapper = new KeywordFieldMapper.Builder("field").ignoreAbove(4).build(context).fieldType();
        assertEquals(Collections.emptyList(), fetchSourceValue(ignoreAboveMapper, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(ignoreAboveMapper, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(ignoreAboveMapper, true));

        MappedFieldType normalizerMapper = new KeywordFieldMapper.Builder("field", createIndexAnalyzers()).normalizer("lowercase")
            .build(context)
            .fieldType();
        assertEquals(Collections.singletonList("value"), fetchSourceValue(normalizerMapper, "VALUE"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(normalizerMapper, 42L));
        assertEquals(Collections.singletonList("value"), fetchSourceValue(normalizerMapper, "value"));

        MappedFieldType nullValueMapper = new KeywordFieldMapper.Builder("field").nullValue("NULL").build(context).fieldType();
        assertEquals(Collections.singletonList("NULL"), fetchSourceValue(nullValueMapper, null));
    }

    private static IndexAnalyzers createIndexAnalyzers() {
        return new IndexAnalyzers(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.ofEntries(
                Map.entry("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.entry("other_lowercase", new NamedAnalyzer("other_lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()))
            ),
            Map.of(
                "lowercase",
                new NamedAnalyzer(
                    "lowercase",
                    AnalyzerScope.INDEX,
                    new CustomAnalyzer(
                        TokenizerFactory.newFactory("lowercase", WhitespaceTokenizer::new),
                        new CharFilterFactory[0],
                        new TokenFilterFactory[] { new TokenFilterFactory() {

                            @Override
                            public String name() {
                                return "lowercase";
                            }

                            @Override
                            public TokenStream create(TokenStream tokenStream) {
                                return new org.apache.lucene.analysis.core.LowerCaseFilter(tokenStream);
                            }
                        } }
                    )
                )
            )
        );
    }
}
