/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SmallFloat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VisibleSubsetStatisticsTests extends OpenSearchTestCase {

    private static final String[] FIELDS = { "body", "stacked", "docs_only" };

    /** Splits on whitespace and stacks a synonym on every "car", so some tokens share a position. */
    private static Analyzer analyzer() throws IOException {
        SynonymMap.Builder synonyms = new SynonymMap.Builder(true);
        synonyms.add(new CharsRef("car"), new CharsRef("automobile"), true);
        final SynonymMap map = synonyms.build();
        // Per field, or the components built for the first field would be reused for "stacked" too.
        return new Analyzer(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = new WhitespaceTokenizer();
                TokenStream stream = "stacked".equals(fieldName) ? new SynonymGraphFilter(tokenizer, map, true) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
    }

    private static final FieldType DOCS_ONLY = new FieldType(TextField.TYPE_NOT_STORED);
    static {
        DOCS_ONLY.setIndexOptions(IndexOptions.DOCS);
        DOCS_ONLY.freeze();
    }

    /** Documents long enough that their norms no longer hold the exact length. */
    private List<Document> corpus() {
        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            StringBuilder text = new StringBuilder();
            int length = 30 + random().nextInt(470);
            for (int t = 0; t < length; t++) {
                text.append(random().nextInt(10) == 0 ? "car" : "w" + random().nextInt(200)).append(' ');
            }
            Document doc = new Document();
            doc.add(new StringField("dept", i % 3 == 0 ? "visible" : "hidden", Field.Store.NO));
            doc.add(new TextField("body", text.toString(), Field.Store.NO));
            doc.add(new TextField("stacked", text.toString(), Field.Store.NO));
            doc.add(new Field("docs_only", text.toString(), DOCS_ONLY));
            docs.add(doc);
        }
        return docs;
    }

    private static DirectoryReader index(Directory dir, List<Document> docs) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(analyzer()))) {
            for (int i = 0; i < docs.size(); i++) {
                writer.addDocument(docs.get(i));
                if (i % 170 == 169) {
                    writer.commit();
                }
            }
        }
        return DirectoryReader.open(dir);
    }

    /** Sums each segment's contribution for {@code field} over the documents {@code filter} admits. */
    private static long[] scoped(DirectoryReader reader, Query filter, String field) throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight weight = searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        long[] total = new long[3];
        for (LeafReaderContext ctx : reader.leaves()) {
            BitSet visible = VisibleSubsetStatistics.computeVisibleBitSet(weight, ctx);
            if (visible != null) {
                long[] part = VisibleSubsetStatistics.computeFieldContribution(ctx, visible, field);
                for (int i = 0; i < 3; i++) {
                    total[i] += part[i];
                }
            }
        }
        return total;
    }

    private static long[] lucene(DirectoryReader reader, String field) throws IOException {
        Terms terms = MultiTerms.getTerms(reader, field);
        return new long[] { terms.getDocCount(), terms.getSumTotalTermFreq(), terms.getSumDocFreq() };
    }

    /** A field without term frequencies is counted from the postings, so it matches Lucene exactly. */
    public void testAFieldWithoutFrequenciesIsExact() throws IOException {
        try (Directory dir = newDirectory(); DirectoryReader reader = index(dir, corpus())) {
            assertArrayEquals(lucene(reader, "docs_only"), scoped(reader, new MatchAllDocsQuery(), "docs_only"));
        }
    }

    /**
     * Long documents: the document count is exact, and the summed length, read from norms at the middle of each
     * bucket, is within 1 percent of Lucene's, both for every document and for a view against a physically
     * filtered index.
     */
    public void testLongDocumentsAreWithinOnePercentOfLucene() throws IOException {
        List<Document> docs = corpus();
        List<Document> visibleOnly = new ArrayList<>();
        for (Document doc : docs) {
            if ("visible".equals(doc.getField("dept").stringValue())) {
                visibleOnly.add(doc);
            }
        }
        try (
            Directory dir = newDirectory();
            DirectoryReader reader = index(dir, docs);
            Directory refDir = newDirectory();
            DirectoryReader reference = index(refDir, visibleOnly)
        ) {
            assertWithinOnePercent(lucene(reader, "body"), scoped(reader, new MatchAllDocsQuery(), "body"));
            assertWithinOnePercent(lucene(reference, "body"), scoped(reader, new TermQuery(new Term("dept", "visible")), "body"));
        }
    }

    private static void assertWithinOnePercent(long[] expected, long[] actual) {
        assertEquals("docCount", expected[0], actual[0]);
        assertEquals("sumTotalTermFreq", expected[1], actual[1], expected[1] * 0.01);
        assertEquals("sumDocFreq is reported as its lower bound", actual[0], actual[2]);
    }

    /** Norms leave out tokens that share a position, so a field with stacked synonyms reads shorter than Lucene's. */
    public void testStackedTokensAreNotCounted() throws IOException {
        try (Directory dir = newDirectory(); DirectoryReader reader = index(dir, corpus())) {
            long[] expected = lucene(reader, "stacked");
            long[] actual = scoped(reader, new MatchAllDocsQuery(), "stacked");
            assertEquals(expected[0], actual[0]);
            assertTrue("stacked tokens are left out", actual[1] < expected[1]);
            assertEquals("the field without synonyms has the same length", scoped(reader, new MatchAllDocsQuery(), "body")[1], actual[1]);
        }
    }

    /** The statistics of a view read only its documents, so documents outside it can change without moving them. */
    public void testDocumentsOutsideTheViewDoNotChangeItsStatistics() throws IOException {
        List<Document> docs = corpus();
        List<Document> longerOutside = new ArrayList<>();
        for (Document doc : docs) {
            if ("visible".equals(doc.getField("dept").stringValue())) {
                longerOutside.add(doc);
            } else {
                Document longer = new Document();
                longer.add(new StringField("dept", "hidden", Field.Store.NO));
                String text = "car ".repeat(800);
                longer.add(new TextField("body", text, Field.Store.NO));
                longer.add(new TextField("stacked", text, Field.Store.NO));
                longer.add(new Field("docs_only", text, DOCS_ONLY));
                longerOutside.add(longer);
            }
        }
        Query view = new TermQuery(new Term("dept", "visible"));
        try (
            Directory dir = newDirectory();
            DirectoryReader reader = index(dir, docs);
            Directory otherDir = newDirectory();
            DirectoryReader other = index(otherDir, longerOutside)
        ) {
            for (String field : FIELDS) {
                assertArrayEquals(field, scoped(reader, view, field), scoped(other, view, field));
            }
        }
    }

    public void testANormCountsAsTheMiddleOfItsBucket() {
        for (int length = 1; length < 24; length++) {
            assertEquals(2L * length, VisibleSubsetStatistics.normLengthTimesTwo(SmallFloat.intToByte4(length)));
        }
        for (int length = 24; length < 100_000; length++) {
            byte norm = SmallFloat.intToByte4(length);
            long low = SmallFloat.byte4ToInt(norm);
            long doubled = VisibleSubsetStatistics.normLengthTimesTwo(norm);
            assertTrue("the middle is not below the bucket", doubled >= 2 * low);
            assertTrue(
                "the middle is within the bucket",
                length >= low && length <= SmallFloat.byte4ToInt((byte) (Byte.toUnsignedInt(norm) + 1)) - 1
            );
        }
    }

    public void testNestedChildrenFollowTheirRootDocument() {
        // Two blocks: children 0 and 1 with root 2, then child 3 with root 4.
        FixedBitSet parents = new FixedBitSet(5);
        parents.set(2);
        parents.set(4);
        FixedBitSet firstRootAndASecondBlockChild = new FixedBitSet(5);
        firstRootAndASecondBlockChild.set(2);
        firstRootAndASecondBlockChild.set(3);
        BitSet expanded = VisibleSubsetStatistics.withNestedChildren(firstRootAndASecondBlockChild, parents);
        assertTrue(expanded.get(0) && expanded.get(1) && expanded.get(2));
        assertFalse("a child is visible only through its root", expanded.get(3));
        assertFalse(expanded.get(4));

        FixedBitSet secondRoot = new FixedBitSet(5);
        secondRoot.set(4);
        BitSet second = VisibleSubsetStatistics.withNestedChildren(secondRoot, parents);
        assertEquals(2, second.cardinality());
        assertTrue(second.get(3) && second.get(4));

        FixedBitSet onlyAChild = new FixedBitSet(5);
        onlyAChild.set(0);
        assertNull(VisibleSubsetStatistics.withNestedChildren(onlyAChild, parents));
    }
}
