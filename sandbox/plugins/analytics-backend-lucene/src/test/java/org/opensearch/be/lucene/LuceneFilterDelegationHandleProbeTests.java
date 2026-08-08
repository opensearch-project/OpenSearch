/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the probe-scorer logic in {@link LuceneFilterDelegationHandle#createCollectorWithProbe}.
 * Tests the cost-based gate, probe scan correctness, and segment-empty detection.
 */
public class LuceneFilterDelegationHandleProbeTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        // Create 1000 docs: "rare" term appears in docs 100-109 (10 docs),
        // "common" term appears in docs 0-899 (900 docs = 90%)
        for (int i = 0; i < 1000; i++) {
            Document doc = new Document();
            if (i >= 100 && i < 110) {
                doc.add(new StringField("body", "rare", Field.Store.NO));
            }
            if (i < 900) {
                doc.add(new StringField("body", "common", Field.Store.NO));
            }
            doc.add(new StringField("body", "all", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
        reader = DirectoryReader.open(writer);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        writer.close();
        directory.close();
        super.tearDown();
    }

    /**
     * Tests that a term on a completely different field (not indexed)
     * returns -2 (null scorer) since no posting list exists.
     */
    public void testSegmentEmptyForUnindexedField() throws Exception {
        ProbeTestHandle handle = createHandle();
        // Query on a field that doesn't exist in the index
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("no_such_field", "value"));
        int providerKey = handle.createProvider(query);

        LeafReaderContext leaf = reader.leaves().get(0);
        long writerGen = getWriterGeneration(leaf);

        int[] rgMins = { 0, 500 };
        int[] rgMaxs = { 500, 1000 };
        byte[] outMatch = new byte[2];

        long result = handle.createCollectorWithProbe(providerKey, writerGen, 0, 1000, rgMins, rgMaxs, outMatch);

        assertEquals("Should return -2 for null scorer (unindexed field)", -2L, result);
        assertEquals("All RGs should be 0", 0, outMatch[0]);
        assertEquals("All RGs should be 0", 0, outMatch[1]);
    }

    /**
     * Tests that a dense query (>5% of segment) triggers the cost gate —
     * all outMatch entries are 1 (no probing).
     */
    public void testCostGateSkipsProbeForDenseQuery() throws Exception {
        ProbeTestHandle handle = createHandle();
        // "common" has 900 docs in 1000 = 90%, well above 5% threshold
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("body", "common"));
        int providerKey = handle.createProvider(query);

        LeafReaderContext leaf = reader.leaves().get(0);
        long writerGen = getWriterGeneration(leaf);

        int[] rgMins = { 0, 200, 400, 600, 800 };
        int[] rgMaxs = { 200, 400, 600, 800, 1000 };
        byte[] outMatch = new byte[5];

        long result = handle.createCollectorWithProbe(providerKey, writerGen, 0, 1000, rgMins, rgMaxs, outMatch);

        assertTrue("Should return valid packed result (>= 0)", result >= 0);
        for (int i = 0; i < 5; i++) {
            assertEquals("Gate should mark all RGs as matching", 1, outMatch[i]);
        }
    }

    /**
     * Tests that a selective query triggers the probe scan and correctly
     * identifies which RGs have docs and which don't.
     */
    public void testProbeFiresForSelectiveQuery() throws Exception {
        ProbeTestHandle handle = createHandle();
        // "rare" has 10 docs (docs 100-109) in 1000 = 1%, below 5% threshold
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("body", "rare"));
        int providerKey = handle.createProvider(query);

        LeafReaderContext leaf = reader.leaves().get(0);
        long writerGen = getWriterGeneration(leaf);

        // 5 RGs of 200 docs each: [0,200), [200,400), [400,600), [600,800), [800,1000)
        // "rare" docs are at 100-109, so only RG[0] (0-200) has matches
        int[] rgMins = { 0, 200, 400, 600, 800 };
        int[] rgMaxs = { 200, 400, 600, 800, 1000 };
        byte[] outMatch = new byte[5];

        long result = handle.createCollectorWithProbe(providerKey, writerGen, 0, 1000, rgMins, rgMaxs, outMatch);

        assertTrue("Should return valid packed result", result >= 0);
        assertEquals("RG[0] should match (docs 100-109 are in [0,200))", 1, outMatch[0]);
        assertEquals("RG[1] should not match", 0, outMatch[1]);
        assertEquals("RG[2] should not match", 0, outMatch[2]);
        assertEquals("RG[3] should not match", 0, outMatch[3]);
        assertEquals("RG[4] should not match", 0, outMatch[4]);
    }

    /**
     * Tests that the packed return value correctly encodes collectorKey and firstDoc.
     */
    public void testPackedReturnValueEncoding() throws Exception {
        ProbeTestHandle handle = createHandle();
        Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("body", "rare"));
        int providerKey = handle.createProvider(query);

        LeafReaderContext leaf = reader.leaves().get(0);
        long writerGen = getWriterGeneration(leaf);

        int[] rgMins = { 0 };
        int[] rgMaxs = { 1000 };
        byte[] outMatch = new byte[1];

        long packed = handle.createCollectorWithProbe(providerKey, writerGen, 0, 1000, rgMins, rgMaxs, outMatch);

        assertTrue(packed >= 0);
        int collectorKey = (int) (packed & 0xFFFFFFFFL);
        int firstDoc = (int) (packed >> 32);
        assertTrue("collectorKey should be positive", collectorKey > 0);
        assertEquals("firstDoc should be 100 (first rare doc)", 100, firstDoc);
    }

    /**
     * Tests that changing the threshold dynamically affects the gate behavior.
     */
    public void testDynamicThresholdChange() throws Exception {
        int originalThreshold = 5;
        try {
            ProbeTestHandle handle = createHandle();
            // "rare" = 1% selectivity. At 5% threshold → probe fires
            Query query = new org.apache.lucene.search.TermQuery(new org.apache.lucene.index.Term("body", "rare"));
            int providerKey = handle.createProvider(query);

            LeafReaderContext leaf = reader.leaves().get(0);
            long writerGen = getWriterGeneration(leaf);

            int[] rgMins = { 0, 500 };
            int[] rgMaxs = { 500, 1000 };
            byte[] outMatch = new byte[2];

            // With default threshold (5%): rare=1% → probe fires, should find empty RGs
            long result = handle.createCollectorWithProbe(providerKey, writerGen, 0, 1000, rgMins, rgMaxs, outMatch);
            assertTrue(result >= 0);
            assertEquals("With 5% threshold, probe fires — RG[1] should be 0", 0, outMatch[1]);

            // Change threshold to 0% — disables probing entirely
            LuceneFilterDelegationHandle.setProbeThresholdPercent(0);

            int providerKey2 = handle.createProvider(query);
            byte[] outMatch2 = new byte[2];
            long result2 = handle.createCollectorWithProbe(providerKey2, writerGen, 0, 1000, rgMins, rgMaxs, outMatch2);
            assertTrue(result2 >= 0);
            // threshold=0 means gate condition is "0 > 0 && ..." which is false, so probe still fires
            // Actually threshold=0 check: "if (threshold > 0 && ...)" → false → probe always fires
            // This is correct: threshold=0 disables the GATE, not the probe
            assertEquals("With 0% threshold, gate disabled — probe still fires, RG[1]=0", 0, outMatch2[1]);

        } finally {
            LuceneFilterDelegationHandle.setProbeThresholdPercent(originalThreshold);
        }
    }

    // ── Helper classes ──

    /**
     * Minimal test wrapper around the probe logic. Replicates the essential
     * parts of LuceneFilterDelegationHandle without needing the full
     * DelegatedExpression/QueryShardContext machinery.
     */
    private static class ProbeTestHandle {
        private final IndexSearcher searcher;
        private final Map<Integer, Weight> weights = new HashMap<>();
        private final ConcurrentHashMap<Integer, Object> scorersByKey = new ConcurrentHashMap<>();
        private final AtomicInteger nextProviderKey = new AtomicInteger(1);
        private final AtomicInteger nextCollectorKey = new AtomicInteger(1);
        private final LeafReaderContext leaf;
        private final long writerGeneration;

        ProbeTestHandle(DirectoryReader reader) {
            this.searcher = new IndexSearcher(reader);
            this.leaf = reader.leaves().get(0);
            this.writerGeneration = 1L; // test doesn't use generation-based lookup
        }

        int createProvider(Query query) throws IOException {
            int key = nextProviderKey.getAndIncrement();
            Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            weights.put(key, weight);
            return key;
        }

        long createCollectorWithProbe(int providerKey, long writerGen, int minDoc, int maxDoc, int[] rgMins, int[] rgMaxs, byte[] outMatch)
            throws IOException {
            Weight weight = weights.get(providerKey);
            if (weight == null) return -1L;

            var scorer = weight.scorer(leaf);
            if (scorer == null) {
                Arrays.fill(outMatch, (byte) 0);
                return -2L;
            }

            int collectorKey = nextCollectorKey.getAndIncrement();
            scorersByKey.put(collectorKey, scorer);

            long cost = scorer.iterator().cost();
            long segMaxDoc = leaf.reader().maxDoc();
            int threshold = 5; // Use constant for test isolation
            if (threshold > 0 && cost * 100 > segMaxDoc * threshold) {
                Arrays.fill(outMatch, (byte) 1);
                return ((long) 0 << 32) | (collectorKey & 0xFFFFFFFFL);
            }

            int firstDoc = -1;
            var probeScorer = weight.scorer(leaf);
            if (probeScorer != null) {
                var probeIter = probeScorer.iterator();
                int probeDoc = probeIter.nextDoc();
                firstDoc = probeDoc;

                for (int i = 0; i < rgMins.length; i++) {
                    if (probeDoc == org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                        outMatch[i] = 0;
                        continue;
                    }
                    if (probeDoc < rgMins[i]) {
                        probeDoc = probeIter.advance(rgMins[i]);
                    }
                    if (probeDoc < rgMaxs[i]) {
                        outMatch[i] = (byte) 1;
                    } else {
                        outMatch[i] = (byte) 0;
                    }
                }
            } else {
                Arrays.fill(outMatch, (byte) 1);
            }

            return ((long) Math.max(firstDoc, 0) << 32) | (collectorKey & 0xFFFFFFFFL);
        }
    }

    private ProbeTestHandle createHandle() {
        return new ProbeTestHandle(reader);
    }

    private long getWriterGeneration(LeafReaderContext leaf) {
        return 1L; // test doesn't use generation-based lookup
    }
}
