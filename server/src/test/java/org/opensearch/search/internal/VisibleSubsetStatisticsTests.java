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
        return new Analyzer() {
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
        for (int i = 0; i < 60; i++) {
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
                if (i % 17 == 16) {
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

    public void testEveryDocumentGivesLuceneFieldStatistics() throws IOException {
        try (Directory dir = newDirectory(); DirectoryReader reader = index(dir, corpus())) {
            for (String field : FIELDS) {
                assertArrayEquals(field, lucene(reader, field), scoped(reader, new MatchAllDocsQuery(), field));
            }
        }
    }

    public void testAViewGivesTheStatisticsOfAPhysicallyFilteredIndex() throws IOException {
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
            for (String field : FIELDS) {
                assertArrayEquals(field, lucene(reference, field), scoped(reader, new TermQuery(new Term("dept", "visible")), field));
            }
        }
    }
}
