/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Unit tests for {@link RankDocsQuery} and {@link RankDocsSortField}: score fidelity, per-shard
 * filtering, cross-index {@code _id} disambiguation, missing-doc omission, and position sort.
 */
public class RankDocsQueryTests extends OpenSearchTestCase {

    private static final String INDEX = "products";
    private static final int SHARD = 0;

    /** Index {@code ids} as documents whose only field is the encoded {@code _id}, one doc per id. */
    private static void indexIds(RandomIndexWriter w, List<String> ids) throws IOException {
        for (String id : ids) {
            Document doc = new Document();
            doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
            w.addDocument(doc);
        }
    }

    private static RankDoc doc(String id, float score, int position) {
        return new RankDoc(INDEX, SHARD, id, score, position);
    }

    public void testScoreFidelity() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c", "d", "e"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.95f, 0), doc("c", 0.82f, 1), doc("e", 0.40f, 2));
            RankDocsQuery q = new RankDocsQuery(window, INDEX, SHARD);

            TopDocs td = searcher.search(q, 10);
            assertEquals(3, td.totalHits.value());

            for (ScoreDoc sd : td.scoreDocs) {
                boolean matched = false;
                for (RankDoc rd : window) {
                    if (Float.compare(rd.score(), sd.score) == 0) {
                        matched = true;
                        break;
                    }
                }
                assertTrue("score " + sd.score + " not a pinned score", matched);
            }
            reader.close();
        }
    }

    public void testMissingDocsAreOmitted() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // "zzz" and "missing" are not in the index -> should be silently dropped.
            List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("missing", 0.8f, 1), doc("b", 0.7f, 2), doc("zzz", 0.6f, 3));
            TopDocs td = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10);
            assertEquals(2, td.totalHits.value());
            reader.close();
        }
    }

    public void testPerShardFilteringDropsOtherShardsAndIndices() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Window mixes this shard (products/0), another shard (products/1), and another index (reviews/0).
            List<RankDoc> window = List.of(
                new RankDoc("products", 0, "a", 0.9f, 0),
                new RankDoc("products", 1, "b", 0.8f, 1), // different shard -> filtered out
                new RankDoc("reviews", 0, "c", 0.7f, 2)   // different index -> filtered out
            );

            // Filtering to (products, 0) is the builder's job (doToQuery); the query then only holds "a".
            List<RankDoc> scoped = RankDocsQueryBuilder.filterToShard(window, "products", 0);
            assertEquals(1, scoped.size());

            RankDocsQuery q = new RankDocsQuery(scoped, "products", 0);
            TopDocs td = searcher.search(q, 10);
            assertEquals(1, td.totalHits.value());
            assertEquals(0.9f, td.scoreDocs[0].score, 0.0f);
            reader.close();
        }
    }

    public void testCrossIndexIdCollisionNotMisscored() throws Exception {
        // Same _id "1" exists physically in this segment, but the window entry for "1" belongs to a
        // DIFFERENT index; the builder's shard filter must drop it so it is never scored on (products/0).
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("1", "2"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(
                new RankDoc("reviews", 0, "1", 0.99f, 0), // collision: id "1" but wrong index
                new RankDoc("products", 0, "2", 0.50f, 1) // correct
            );
            List<RankDoc> scoped = RankDocsQueryBuilder.filterToShard(window, "products", 0);
            assertEquals(1, scoped.size());

            RankDocsQuery q = new RankDocsQuery(scoped, "products", 0);
            TopDocs td = searcher.search(q, 10);
            assertEquals(1, td.totalHits.value());
            assertEquals("only products/0 id 2 should match", 0.50f, td.scoreDocs[0].score, 0.0f);
            reader.close();
        }
    }

    public void testSortByPositionOrdersByRankNotScore() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            // Positions intentionally inverted vs scores: "c" has the lowest score but position 0 (top).
            List<RankDoc> window = List.of(doc("a", 0.90f, 2), doc("b", 0.80f, 1), doc("c", 0.10f, 0));

            RankDocsQuery q = new RankDocsQuery(window, INDEX, SHARD);
            Sort sort = new Sort(new RankDocsSortField(window, INDEX, SHARD));
            TopDocs td = searcher.search(q, 10, sort);
            assertEquals(3, td.totalHits.value());

            int[] positions = new int[td.scoreDocs.length];
            for (int i = 0; i < td.scoreDocs.length; i++) {
                positions[i] = positionOf(searcher, td.scoreDocs[i].doc, window);
            }
            for (int i = 1; i < positions.length; i++) {
                assertTrue("positions not ascending: " + positions[i - 1] + " !<= " + positions[i], positions[i - 1] <= positions[i]);
            }
            // Top hit must be position 0 ("c"), despite its low score.
            assertEquals(0, positions[0]);
            reader.close();
        }
    }

    /** Resolve a hit's retriever position by matching its stored _id back to the window. */
    private int positionOf(IndexSearcher searcher, int docId, List<RankDoc> window) throws IOException {
        for (RankDoc rd : window) {
            TermQuery tq = new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(rd.id())));
            TopDocs td = searcher.search(new ConstantScoreQuery(tq), 1);
            if (td.scoreDocs.length > 0 && td.scoreDocs[0].doc == docId) {
                return rd.position();
            }
        }
        return RankDocsSortField.UNPINNED_POSITION;
    }

    public void testExplainIsUnsupported() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            RankDocsQuery q = new RankDocsQuery(List.of(doc("a", 0.9f, 0)), INDEX, SHARD);
            Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
            LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
            // RankDocsQuery replays a coordinator-computed ranking and has nothing to explain.
            expectThrows(UnsupportedOperationException.class, () -> weight.explain(leaf, 0));
            reader.close();
        }
    }

    public void testEmptyWindowMatchesNothing() throws Exception {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            RankDocsQuery q = new RankDocsQuery(List.of(), INDEX, SHARD);
            TopDocs td = searcher.search(q, 10);
            assertEquals(0, td.totalHits.value());
            reader.close();
        }
    }

    public void testBoolConstantScoreBaselineMatchesSameDocs() throws Exception {
        // Sanity: the bool-constant-score alternative selects the same docs (score semantics differ).
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir, new KeywordAnalyzer());
            indexIds(w, List.of("a", "b", "c", "d"));
            IndexReader reader = w.getReader();
            w.close();
            IndexSearcher searcher = newSearcher(reader);

            List<RankDoc> window = List.of(doc("a", 0.9f, 0), doc("c", 0.7f, 1));
            long rank = searcher.search(new RankDocsQuery(window, INDEX, SHARD), 10).totalHits.value();

            BooleanQuery.Builder bool = new BooleanQuery.Builder();
            for (RankDoc rd : window) {
                TermQuery tq = new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId(rd.id())));
                bool.add(new BoostQuery(new ConstantScoreQuery(tq), rd.score()), BooleanClause.Occur.SHOULD);
            }
            long boolCount = searcher.search(bool.build(), 10).totalHits.value();
            assertEquals(rank, boolCount);
            reader.close();
        }
    }
}
