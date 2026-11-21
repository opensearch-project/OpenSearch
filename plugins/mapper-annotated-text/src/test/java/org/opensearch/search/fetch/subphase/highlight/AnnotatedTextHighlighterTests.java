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

package org.opensearch.search.fetch.subphase.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.uhighlight.CustomSeparatorBreakIterator;
import org.apache.lucene.search.uhighlight.SplittingBreakIterator;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.core.common.Strings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedHighlighterAnalyzer;
import org.opensearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotatedText;
import org.opensearch.index.mapper.annotatedtext.AnnotatedTextFieldMapper.AnnotationAnalyzerWrapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.lucene.search.uhighlight.CustomUnifiedHighlighter;
import org.opensearch.lucene.search.uhighlight.Snippet;
import org.opensearch.plugin.mapper.AnnotatedTextPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.net.URLEncoder;
import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static org.opensearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AnnotatedTextHighlighterTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new AnnotatedTextPlugin());
    }

    private void assertHighlightOneDoc(
        String fieldName,
        String[] markedUpInputs,
        Query query,
        Locale locale,
        BreakIterator breakIterator,
        int noMatchSize,
        String[] expectedPassages
    ) throws Exception {

        // Annotated fields wrap the usual analyzer with one that injects extra tokens
        Analyzer wrapperAnalyzer = new AnnotationAnalyzerWrapper(new StandardAnalyzer());
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(wrapperAnalyzer);
        iwc.setMergePolicy(newTieredMergePolicy(random()));
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        FieldType ft = new FieldType(TextField.TYPE_STORED);
        if (randomBoolean()) {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        } else {
            ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        }
        ft.freeze();
        Document doc = new Document();
        for (String input : markedUpInputs) {
            Field field = new Field(fieldName, "", ft);
            field.setStringValue(input);
            doc.add(field);
        }
        iw.addDocument(doc);
        DirectoryReader reader = iw.getReader();
        IndexSearcher searcher = newSearcher(reader);
        iw.close();

        AnnotatedText[] annotations = new AnnotatedText[markedUpInputs.length];
        for (int i = 0; i < markedUpInputs.length; i++) {
            annotations[i] = AnnotatedText.parse(markedUpInputs[i]);
        }
        AnnotatedHighlighterAnalyzer hiliteAnalyzer = new AnnotatedHighlighterAnalyzer(wrapperAnalyzer);
        hiliteAnalyzer.setAnnotations(annotations);
        AnnotatedPassageFormatter passageFormatter = new AnnotatedPassageFormatter(new DefaultEncoder());
        passageFormatter.setAnnotations(annotations);

        ArrayList<Object> plainTextForHighlighter = new ArrayList<>(annotations.length);
        for (int i = 0; i < annotations.length; i++) {
            plainTextForHighlighter.add(annotations[i].textMinusMarkup);
        }

        TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 1, Sort.INDEXORDER);
        assertThat(topDocs.totalHits.value(), equalTo(1L));
        String rawValue = Strings.collectionToDelimitedString(plainTextForHighlighter, String.valueOf(MULTIVAL_SEP_CHAR));
        UnifiedHighlighter.Builder builder = UnifiedHighlighter.builder(searcher, hiliteAnalyzer);
        builder.withFieldMatcher("text"::equals);
        CustomUnifiedHighlighter highlighter = new CustomUnifiedHighlighter(
            builder,
            null,
            passageFormatter,
            locale,
            breakIterator,
            "index",
            "text",
            query,
            noMatchSize,
            expectedPassages.length,
            Integer.MAX_VALUE,
            null
        );
        final Snippet[] snippets = highlighter.highlightField(getOnlyLeafReader(reader), topDocs.scoreDocs[0].doc, () -> rawValue);
        assertEquals(expectedPassages.length, snippets.length);
        for (int i = 0; i < snippets.length; i++) {
            assertEquals(expectedPassages[i], snippets[i].getText());
        }
        reader.close();
        dir.close();
    }

    public void testAnnotatedTextStructuredMatch() throws Exception {
        // Check that a structured token eg a URL can be highlighted in a query
        // on marked-up
        // content using an "annotated_text" type field.
        String url = "https://en.wikipedia.org/wiki/Key_Word_in_Context";
        String encodedUrl = URLEncoder.encode(url, "UTF-8");
        String annotatedWord = "[highlighting](" + encodedUrl + ")";
        String highlightedAnnotatedWord = "[highlighting]("
            + AnnotatedPassageFormatter.SEARCH_HIT_TYPE
            + "="
            + encodedUrl
            + "&"
            + encodedUrl
            + ")";
        final String[] markedUpInputs = {
            "This is a test. Just a test1 " + annotatedWord + " from [annotated](bar) highlighter.",
            "This is the second " + annotatedWord + " value to perform highlighting on a longer text that gets scored lower." };

        String[] expectedPassages = {
            "This is a test. Just a test1 " + highlightedAnnotatedWord + " from [annotated](bar) highlighter.",
            "This is the second "
                + highlightedAnnotatedWord
                + " value to perform highlighting on a"
                + " longer text that gets scored lower." };
        Query query = new TermQuery(new Term("text", url));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextOverlapsWithUnstructuredSearchTerms() throws Exception {
        final String[] markedUpInputs = {
            "[Donald Trump](Donald+Trump) visited Singapore",
            "Donald duck is a [Disney](Disney+Inc) invention" };

        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextMultiFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = {
            "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald",
            "Donald duck is a [Disney](Disney+Inc) invention" };
        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "Kim shook hands with [Donald](_hit_term=donald)",
            "[Donald](_hit_term=donald) duck is a [Disney](Disney+Inc) invention" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextSingleFieldWithBreakIterator() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore. Kim shook hands with Donald" };
        String[] expectedPassages = {
            "[Donald](_hit_term=donald) Trump visited Singapore",
            "Kim shook hands with [Donald](_hit_term=donald)" };
        Query query = new TermQuery(new Term("text", "donald"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        breakIterator = new SplittingBreakIterator(breakIterator, '.');
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextSingleFieldWithPhraseQuery() throws Exception {
        final String[] markedUpInputs = { "[Donald Trump](Donald+Trump) visited Singapore", "Donald Jr was with Melania Trump" };
        String[] expectedPassages = { "[Donald](_hit_term=donald) [Trump](_hit_term=trump) visited Singapore" };
        Query query = new PhraseQuery("text", "donald", "trump");
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testBadAnnotation() throws Exception {
        final String[] markedUpInputs = { "Missing bracket for [Donald Trump](Donald+Trump visited Singapore" };
        String[] expectedPassages = { "Missing bracket for [Donald Trump](Donald+Trump visited [Singapore](_hit_term=singapore)" };
        Query query = new TermQuery(new Term("text", "singapore"));
        BreakIterator breakIterator = new CustomSeparatorBreakIterator(MULTIVAL_SEP_CHAR);
        assertHighlightOneDoc("text", markedUpInputs, query, Locale.ROOT, breakIterator, 0, expectedPassages);
    }

    public void testAnnotatedTextWithMaxAnalyzerOffset() throws IOException {
        assertHighlightWithMaxAnalyzerOffset("no", "project", 100, "[OpenSearch](_hit_term=project&project) is a search engine.");
        assertHighlightWithMaxAnalyzerOffset("no", "engine", 100, "[OpenSearch](project) is a search [engine](_hit_term=engine).");
        assertHighlightWithMaxAnalyzerOffset("no", "engine", 10, null);
    }

    public void testAnnotatedTextWithMaxAnalyzerOffsetAndTermVectors() throws IOException {
        assertHighlightWithMaxAnalyzerOffset(
            "with_positions_offsets",
            "project",
            100,
            "[OpenSearch](_hit_term=project&project) is a search engine."
        );
    }

    private void assertHighlightWithMaxAnalyzerOffset(String termVectors, String term, int maxAnalyzerOffset, String expected)
        throws IOException {
        try (
            MapperService mapper = createMapperService(
                fieldMapping(b -> b.field("type", "annotated_text").field("store", true).field("term_vector", termVectors))
            )
        ) {
            withLuceneIndex(
                mapper,
                writer -> writer.addDocument(
                    mapper.documentMapper().parse(source(b -> b.field("field", "[OpenSearch](project) is a search engine."))).rootDoc()
                ),
                reader -> {
                    QueryShardContext shardContext = createQueryShardContext(mapper);
                    SearchHighlightContext.Field field = new HighlightBuilder().highlighterType("annotated")
                        .maxAnalyzerOffset(maxAnalyzerOffset)
                        .numOfFragments(0)
                        .field("field")
                        .build(shardContext)
                        .fields()
                        .iterator()
                        .next();
                    FetchContext fetchContext = mock(FetchContext.class);
                    when(fetchContext.mapperService()).thenReturn(mapper);
                    when(fetchContext.getQueryShardContext()).thenReturn(shardContext);
                    when(fetchContext.getIndexSettings()).thenReturn(mapper.getIndexSettings());
                    try (
                        ContextIndexSearcher searcher = new ContextIndexSearcher(
                            reader,
                            IndexSearcher.getDefaultSimilarity(),
                            null,
                            IndexSearcher.getDefaultQueryCachingPolicy(),
                            false,
                            null,
                            null
                        )
                    ) {
                        when(fetchContext.searcher()).thenReturn(searcher);
                        HitContext hit = new HitContext(new SearchHit(0), reader.leaves().get(0), 0, new SourceLookup());
                        HighlightField highlight = new AnnotatedTextHighlighter().highlight(
                            new FieldHighlightContext(
                                "field",
                                field,
                                mapper.fieldType("field"),
                                fetchContext,
                                hit,
                                new TermQuery(new Term("field", term)),
                                false,
                                new HashMap<>()
                            )
                        );
                        if (expected == null) {
                            assertNull(highlight);
                        } else {
                            assertNotNull(highlight);
                            assertEquals(1, highlight.fragments().length);
                            assertEquals(expected, highlight.fragments()[0].string());
                        }
                    }
                }
            );
        }
    }

}
