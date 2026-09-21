/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RandomSamplingQueryTests extends OpenSearchTestCase {

    public void testEqualsAndHashCode() {
        QueryUtils.checkEqual(new RandomSamplingQuery(0.1, 42), new RandomSamplingQuery(0.1, 42));
        QueryUtils.checkUnequal(new RandomSamplingQuery(0.1, 42), new RandomSamplingQuery(0.2, 42));
        QueryUtils.checkUnequal(new RandomSamplingQuery(0.1, 42), new RandomSamplingQuery(0.1, 43));
    }

    public void testRejectsProbabilityOutsideZeroToOne() {
        for (double probability : new double[] { 0.0, -0.1, 1.1, Double.NaN }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQuery(probability, 42));
            assertThat(e.getMessage(), containsString("probability"));
        }
    }

    /**
     * The number of sampled documents is binomially distributed, so assert it falls within four standard deviations of
     * the expected count rather than asserting an exact number. A tighter assertion would flake.
     */
    public void testSampleSizeIsWithinFourStandardDeviations() throws IOException {
        int numDocs = 20000;
        double probability = 0.1;
        try (Directory dir = newDirectory()) {
            indexDocs(dir, numDocs, 1);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                int count = new IndexSearcher(reader).count(new RandomSamplingQuery(probability, randomInt()));
                double expected = numDocs * probability;
                double sigma = Math.sqrt(numDocs * probability * (1 - probability));
                assertThat((double) count, greaterThan(expected - 4 * sigma));
                assertThat((double) count, lessThan(expected + 4 * sigma));
            }
        }
    }

    public void testSameSeedProducesSameSample() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir, 5000, 1);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                int seed = randomInt();
                assertEquals(
                    sampleBySegment(reader, new RandomSamplingQuery(0.2, seed)),
                    sampleBySegment(reader, new RandomSamplingQuery(0.2, seed))
                );
            }
        }
    }

    public void testDifferentSeedProducesDifferentSample() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir, 5000, 1);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertNotEquals(
                    sampleBySegment(reader, new RandomSamplingQuery(0.2, 1)),
                    sampleBySegment(reader, new RandomSamplingQuery(0.2, 2))
                );
            }
        }
    }

    /**
     * {@code advance} must walk the same predetermined sequence {@code nextDoc} does. If it re-drew the skip
     * distribution from the target, the sampled set would depend on which documents the query matched.
     */
    public void testAdvanceWalksTheSameSequenceAsNextDoc() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir, 5000, 1);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                RandomSamplingQuery query = new RandomSamplingQuery(0.1, randomInt());
                IndexSearcher searcher = new IndexSearcher(reader);
                Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
                for (LeafReaderContext ctx : reader.leaves()) {
                    List<Integer> viaNextDoc = new ArrayList<>(collect(weight, ctx));

                    // Advancing to each sampled document in turn must return exactly that document, and advancing to
                    // the document after it must return the next one in the sequence.
                    DocIdSetIterator advancing = iterator(weight, ctx);
                    for (int i = 0; i < viaNextDoc.size(); i++) {
                        int expected = viaNextDoc.get(i);
                        assertEquals((long) expected, (long) advancing.advance(expected));
                    }
                    DocIdSetIterator skipping = iterator(weight, ctx);
                    if (viaNextDoc.size() > 2) {
                        int target = viaNextDoc.get(viaNextDoc.size() / 2);
                        assertEquals((long) target, (long) skipping.advance(target));
                        assertEquals((long) viaNextDoc.get(viaNextDoc.size() / 2 + 1), (long) skipping.nextDoc());
                    }
                }
            }
        }
    }

    public void testProbabilityOneMatchesEveryDocument() throws IOException {
        int numDocs = 500;
        try (Directory dir = newDirectory()) {
            indexDocs(dir, numDocs, 3);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(numDocs, new IndexSearcher(reader).count(new RandomSamplingQuery(1.0, randomInt())));
            }
        }
    }

    /**
     * The sample of a given segment must not change when that segment's position in the searcher's leaf list changes,
     * which is why the seed is derived from the segment name rather than from {@code LeafReaderContext#ord}.
     */
    public void testSampleIsStableWhenSegmentOrdinalChanges() throws IOException {
        try (Directory dir = newDirectory()) {
            indexDocs(dir, 3000, 3);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertThat(reader.leaves().size(), greaterThan(1));
                RandomSamplingQuery query = new RandomSamplingQuery(0.2, 7);
                Map<String, Set<Integer>> original = sampleBySegment(reader, query);

                LeafReader[] reversed = new LeafReader[reader.leaves().size()];
                for (int i = 0; i < reversed.length; i++) {
                    reversed[i] = reader.leaves().get(reversed.length - 1 - i).reader();
                }
                try (MultiReader shuffled = new MultiReader(reversed, false)) {
                    assertEquals(original, sampleBySegment(shuffled, query));
                }
            }
        }
    }

    /**
     * A reader that cannot be unwrapped to a {@link org.apache.lucene.index.SegmentReader} has no segment name, so
     * seeding falls back to the leaf ordinal. It must not throw.
     */
    public void testFallsBackOnReaderWithoutSegmentName() throws IOException {
        MemoryIndex memoryIndex = new MemoryIndex();
        memoryIndex.addField("field", "value", new StandardAnalyzer());
        IndexSearcher searcher = memoryIndex.createSearcher();
        assertEquals(1, searcher.count(new RandomSamplingQuery(1.0, 42)));
        int sampled = searcher.count(new RandomSamplingQuery(0.4, 42));
        assertThat(sampled, greaterThanOrEqualTo(0));
        assertThat(sampled, lessThanOrEqualTo(1));
    }

    private static void indexDocs(Directory dir, int numDocs, int numSegments) throws IOException {
        try (
            IndexWriter writer = new IndexWriter(
                dir,
                new IndexWriterConfig().setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
            )
        ) {
            int perSegment = Math.max(1, numDocs / numSegments);
            for (int i = 0; i < numDocs; i++) {
                writer.addDocument(new Document());
                if ((i + 1) % perSegment == 0) {
                    writer.commit();
                }
            }
            writer.commit();
        }
    }

    private static Map<String, Set<Integer>> sampleBySegment(IndexReader reader, RandomSamplingQuery query) throws IOException {
        Weight weight = query.createWeight(new IndexSearcher(reader), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Map<String, Set<Integer>> perSegment = new HashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            perSegment.put(Lucene.segmentReader(ctx.reader()).getSegmentName(), collect(weight, ctx));
        }
        return perSegment;
    }

    private static Set<Integer> collect(Weight weight, LeafReaderContext ctx) throws IOException {
        Set<Integer> docs = new LinkedHashSet<>();
        DocIdSetIterator iterator = iterator(weight, ctx);
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            docs.add(doc);
        }
        return docs;
    }

    private static DocIdSetIterator iterator(Weight weight, LeafReaderContext ctx) throws IOException {
        ScorerSupplier supplier = weight.scorerSupplier(ctx);
        assertNotNull(supplier);
        return supplier.get(Long.MAX_VALUE).iterator();
    }
}
