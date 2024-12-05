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

package org.opensearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.CombinedBitSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SparseFixedBitSet;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.SearchService;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.opensearch.search.internal.ContextIndexSearcher.intersectScorerAndBitSet;
import static org.opensearch.search.internal.ExitableDirectoryReader.ExitableLeafReader;
import static org.opensearch.search.internal.ExitableDirectoryReader.ExitablePointValues;
import static org.opensearch.search.internal.ExitableDirectoryReader.ExitableTerms;
import static org.opensearch.search.internal.IndexReaderUtils.getLeaves;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextIndexSearcherTests extends OpenSearchTestCase {
    public void testIntersectScorerAndRoleBits() throws Exception {
        final Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE));

        Document document = new Document();
        document.add(new StringField("field1", "value1", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value2", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value3", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        document = new Document();
        document.add(new StringField("field1", "value4", Field.Store.NO));
        document.add(new StringField("field2", "value1", Field.Store.NO));
        iw.addDocument(document);

        iw.commit();
        iw.deleteDocuments(new Term("field1", "value3"));
        iw.close();
        DirectoryReader directoryReader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(directoryReader);
        Weight weight = searcher.createWeight(
            new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("field2", "value1"))), 3f),
            ScoreMode.COMPLETE,
            1f
        );

        LeafReaderContext leaf = directoryReader.leaves().get(0);

        CombinedBitSet bitSet = new CombinedBitSet(query(leaf, "field1", "value1"), leaf.reader().getLiveDocs());
        LeafCollector leafCollector = new LeafBucketCollector() {
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(0));
                assertThat(scorer.score(), equalTo(3f));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value2"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(1));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value3"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                fail("docId [" + doc + "] should have been deleted");
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        bitSet = new CombinedBitSet(query(leaf, "field1", "value4"), leaf.reader().getLiveDocs());
        leafCollector = new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assertThat(doc, equalTo(3));
            }
        };
        intersectScorerAndBitSet(weight.scorer(leaf), bitSet, leafCollector, () -> {});

        directoryReader.close();
        directory.close();
    }

    public void testContextIndexSearcherSparseNoDeletions() throws IOException {
        doTestContextIndexSearcher(true, false);
    }

    public void testContextIndexSearcherDenseNoDeletions() throws IOException {
        doTestContextIndexSearcher(false, false);
    }

    public void testContextIndexSearcherSparseWithDeletions() throws IOException {
        doTestContextIndexSearcher(true, true);
    }

    public void testContextIndexSearcherDenseWithDeletions() throws IOException {
        doTestContextIndexSearcher(false, true);
    }

    public void doTestContextIndexSearcher(boolean sparse, boolean deletions) throws IOException {
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(null));
        Document doc = new Document();
        StringField allowedField = new StringField("allowed", "yes", Field.Store.NO);
        doc.add(allowedField);
        StringField fooField = new StringField("foo", "bar", Field.Store.NO);
        doc.add(fooField);
        StringField deleteField = new StringField("delete", "no", Field.Store.NO);
        doc.add(deleteField);
        IntPoint pointField = new IntPoint("point", 1, 2);
        doc.add(pointField);
        w.addDocument(doc);
        if (deletions) {
            // add a document that matches foo:bar but will be deleted
            deleteField.setStringValue("yes");
            w.addDocument(doc);
            deleteField.setStringValue("no");
        }
        allowedField.setStringValue("no");
        w.addDocument(doc);
        if (sparse) {
            for (int i = 0; i < 1000; ++i) {
                w.addDocument(doc);
            }
            w.forceMerge(1);
        }
        w.deleteDocuments(new Term("delete", "yes"));

        IndexSettings settings = IndexSettingsModule.newIndexSettings("_index", Settings.EMPTY);
        BitsetFilterCache.Listener listener = new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {

            }

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {

            }
        };
        DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(w), new ShardId(settings.getIndex(), 0));
        BitsetFilterCache cache = new BitsetFilterCache(settings, listener);
        Query roleQuery = new TermQuery(new Term("allowed", "yes"));
        BitSet bitSet = cache.getBitSetProducer(roleQuery).getBitSet(reader.leaves().get(0));
        if (sparse) {
            assertThat(bitSet, instanceOf(SparseFixedBitSet.class));
        } else {
            assertThat(bitSet, instanceOf(FixedBitSet.class));
        }

        DocumentSubsetDirectoryReader filteredReader = new DocumentSubsetDirectoryReader(reader, cache, roleQuery);

        SearchContext searchContext = mock(SearchContext.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(searchContext.indexShard()).thenReturn(indexShard);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);
        when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
        ContextIndexSearcher searcher = new ContextIndexSearcher(
            filteredReader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            true,
            null,
            searchContext
        );

        for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
            assertThat(context.reader(), instanceOf(SequentialStoredFieldsLeafReader.class));
            SequentialStoredFieldsLeafReader lf = (SequentialStoredFieldsLeafReader) context.reader();
            assertNotNull(lf.getSequentialStoredFieldsReader());
        }
        // Assert wrapping
        assertEquals(ExitableDirectoryReader.class, searcher.getIndexReader().getClass());
        for (LeafReaderContext lrc : searcher.getIndexReader().leaves()) {
            assertEquals(ExitableLeafReader.class, lrc.reader().getClass());
            assertNotEquals(ExitableTerms.class, lrc.reader().terms("foo").getClass());
            assertNotEquals(ExitablePointValues.class, lrc.reader().getPointValues("point").getClass());
        }
        searcher.addQueryCancellation(() -> {});
        for (LeafReaderContext lrc : searcher.getIndexReader().leaves()) {
            assertEquals(ExitableTerms.class, lrc.reader().terms("foo").getClass());
            assertEquals(ExitablePointValues.class, lrc.reader().getPointValues("point").getClass());
        }

        // Searching a non-existing term will trigger a null scorer
        assertEquals(0, searcher.count(new TermQuery(new Term("non_existing_field", "non_existing_value"))));

        assertEquals(1, searcher.count(new TermQuery(new Term("foo", "bar"))));

        // make sure scorers are created only once, see #1725
        assertEquals(1, searcher.count(new CreateScorerOnceQuery(new MatchAllDocsQuery())));

        TopDocs topDocs = searcher.search(new BoostQuery(new ConstantScoreQuery(new TermQuery(new Term("foo", "bar"))), 3f), 1);
        assertEquals(1, topDocs.totalHits.value);
        assertEquals(1, topDocs.scoreDocs.length);
        assertEquals(3f, topDocs.scoreDocs[0].score, 0);

        IOUtils.close(reader, w, dir);
    }

    public void testSlicesInternal() throws Exception {
        final List<LeafReaderContext> leaves = getLeaves(10);
        try (
            final Directory directory = newDirectory();
            IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            Document document = new Document();
            document.add(new StringField("field1", "value1", Field.Store.NO));
            document.add(new StringField("field2", "value1", Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                SearchContext searchContext = mock(SearchContext.class);
                IndexShard indexShard = mock(IndexShard.class);
                when(searchContext.indexShard()).thenReturn(indexShard);
                when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
                ContextIndexSearcher searcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    null,
                    searchContext
                );
                // Case 1: Verify the slice count when lucene default slice computation is used
                IndexSearcher.LeafSlice[] slices = searcher.slicesInternal(
                    leaves,
                    SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_DEFAULT_VALUE
                );
                int expectedSliceCount = 2;
                // 2 slices will be created since max segment per slice of 5 will be reached
                assertEquals(expectedSliceCount, slices.length);
                for (int i = 0; i < expectedSliceCount; ++i) {
                    assertEquals(5, slices[i].leaves.length);
                }

                // Case 2: Verify the slice count when custom max slice computation is used
                expectedSliceCount = 4;
                slices = searcher.slicesInternal(leaves, expectedSliceCount);

                // 4 slices will be created with 3 leaves in first 2 slices and 2 leaves in other slices
                assertEquals(expectedSliceCount, slices.length);
                for (int i = 0; i < expectedSliceCount; ++i) {
                    if (i < 2) {
                        assertEquals(3, slices[i].leaves.length);
                    } else {
                        assertEquals(2, slices[i].leaves.length);
                    }
                }
            }
        }
    }

    public void testGetSlicesWithNonNullExecutorButCSDisabled() throws Exception {
        final List<LeafReaderContext> leaves = getLeaves(10);
        try (
            final Directory directory = newDirectory();
            IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            Document document = new Document();
            document.add(new StringField("field1", "value1", Field.Store.NO));
            document.add(new StringField("field2", "value1", Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
            try (DirectoryReader directoryReader = DirectoryReader.open(directory);) {
                SearchContext searchContext = mock(SearchContext.class);
                IndexShard indexShard = mock(IndexShard.class);
                when(searchContext.indexShard()).thenReturn(indexShard);
                when(searchContext.bucketCollectorProcessor()).thenReturn(SearchContext.NO_OP_BUCKET_COLLECTOR_PROCESSOR);
                when(searchContext.shouldUseConcurrentSearch()).thenReturn(false);
                ContextIndexSearcher searcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    null,
                    searchContext
                );
                // Case 1: Verify getSlices returns not null when concurrent segment search is disabled
                assertEquals(1, searcher.getSlices().length);

                // Case 2: Verify the slice count when custom max slice computation is used
                searcher = new ContextIndexSearcher(
                    directoryReader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true,
                    mock(ExecutorService.class),
                    searchContext
                );
                when(searchContext.shouldUseConcurrentSearch()).thenReturn(true);
                when(searchContext.getTargetMaxSliceCount()).thenReturn(4);
                int expectedSliceCount = 4;
                IndexSearcher.LeafSlice[] slices = searcher.slices(leaves);

                // 4 slices will be created with 3 leaves in first 2 slices and 2 leaves in other slices
                assertEquals(expectedSliceCount, slices.length);
                for (int i = 0; i < expectedSliceCount; ++i) {
                    if (i < 2) {
                        assertEquals(3, slices[i].leaves.length);
                    } else {
                        assertEquals(2, slices[i].leaves.length);
                    }
                }
            }
        }
    }

    private SparseFixedBitSet query(LeafReaderContext leaf, String field, String value) throws IOException {
        SparseFixedBitSet sparseFixedBitSet = new SparseFixedBitSet(leaf.reader().maxDoc());
        TermsEnum tenum = leaf.reader().terms(field).iterator();
        while (tenum.next().utf8ToString().equals(value) == false) {
        }
        PostingsEnum penum = tenum.postings(null);
        sparseFixedBitSet.or(penum);
        return sparseFixedBitSet;
    }

    private static class DocumentSubsetDirectoryReader extends FilterDirectoryReader {
        private final BitsetFilterCache bitsetFilterCache;
        private final Query roleQuery;

        DocumentSubsetDirectoryReader(DirectoryReader in, BitsetFilterCache bitsetFilterCache, Query roleQuery) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    try {
                        return new DocumentSubsetReader(reader, bitsetFilterCache, roleQuery);
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToOpenSearchException(e);
                    }
                }
            });
            this.bitsetFilterCache = bitsetFilterCache;
            this.roleQuery = roleQuery;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new DocumentSubsetDirectoryReader(in, bitsetFilterCache, roleQuery);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static class DocumentSubsetReader extends SequentialStoredFieldsLeafReader {
        private final BitSet roleQueryBits;
        private final int numDocs;

        /**
         * <p>Construct a FilterLeafReader based on the specified base reader.
         * <p>Note that base reader is closed if this FilterLeafReader is closed.</p>
         *
         * @param in specified base reader.
         */
        DocumentSubsetReader(LeafReader in, BitsetFilterCache bitsetFilterCache, Query roleQuery) throws IOException {
            super(in);
            this.roleQueryBits = bitsetFilterCache.getBitSetProducer(roleQuery).getBitSet(in.getContext());
            this.numDocs = computeNumDocs(in, roleQueryBits);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            // Not delegated since we change the live docs
            return null;
        }

        @Override
        public int numDocs() {
            return numDocs;
        }

        @Override
        public Bits getLiveDocs() {
            final Bits actualLiveDocs = in.getLiveDocs();
            if (roleQueryBits == null) {
                return new Bits.MatchNoBits(in.maxDoc());
            } else if (actualLiveDocs == null) {
                return roleQueryBits;
            } else {
                // apply deletes when needed:
                return new CombinedBitSet(roleQueryBits, actualLiveDocs);
            }
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        private static int computeNumDocs(LeafReader reader, BitSet roleQueryBits) {
            final Bits liveDocs = reader.getLiveDocs();
            if (roleQueryBits == null) {
                return 0;
            } else if (liveDocs == null) {
                // slow
                return roleQueryBits.cardinality();
            } else {
                // very slow, but necessary in order to be correct
                int numDocs = 0;
                DocIdSetIterator it = new BitSetIterator(roleQueryBits, 0L); // we don't use the cost
                try {
                    for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
                        if (liveDocs.get(doc)) {
                            numDocs++;
                        }
                    }
                    return numDocs;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private static class CreateScorerOnceWeight extends Weight {

        private final Weight weight;
        private final Set<Object> seenLeaves = Collections.newSetFromMap(new IdentityHashMap<>());

        CreateScorerOnceWeight(Weight weight) {
            super(weight.getQuery());
            this.weight = weight;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return weight.explain(context, doc);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheHelper().getKey()));
            return weight.scorer(context);
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
            assertTrue(seenLeaves.add(context.reader().getCoreCacheHelper().getKey()));
            return weight.bulkScorer(context);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    private static class CreateScorerOnceQuery extends Query {

        private final Query query;

        CreateScorerOnceQuery(Query query) {
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return query.toString(field);
        }

        @Override
        public Query rewrite(IndexSearcher searcher) throws IOException {
            Query queryRewritten = query.rewrite(searcher);
            if (query != queryRewritten) {
                return new CreateScorerOnceQuery(queryRewritten);
            }
            return super.rewrite(searcher);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, org.apache.lucene.search.ScoreMode scoreMode, float boost) throws IOException {
            return new CreateScorerOnceWeight(query.createWeight(searcher, scoreMode, boost));
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj) && query.equals(((CreateScorerOnceQuery) obj).query);
        }

        @Override
        public int hashCode() {
            return 31 * classHash() + query.hashCode();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }
    }
}
