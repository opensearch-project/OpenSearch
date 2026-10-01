/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class ExitablePostingsEnumTests extends OpenSearchTestCase {

    /** Run metadata must reach Lucene unchanged without advancing the postings cursor. */
    public void testDocIDRunEndDelegates() throws IOException {
        BulkPostingsEnum delegate = new BulkPostingsEnum(37, 10000, 1, 256);
        PostingsEnum postings = wrap(delegate, new Cancellation());
        assertEquals(10000, postings.docIDRunEnd());
        assertEquals(37, postings.docID());
    }

    /**
     * Bulk fills must honor offsets and exclusive bounds, retain existing bits, and leave the cursor
     * on the first unconsumed document while using the delegate's bulk implementation.
     */
    public void testIntoBitSetPreservesDocsAndCursor() throws IOException {
        for (int step : new int[] { 1, 17, 10000 }) {
            for (int offset : new int[] { -11, 23 }) {
                BulkPostingsEnum delegate = new BulkPostingsEnum(37, 30000, step, 256);
                PostingsEnum postings = wrap(delegate, new Cancellation());
                FixedBitSet bits = new FixedBitSet(30002 - offset);
                FixedBitSet expected = new FixedBitSet(bits.length());
                bits.set(40 - offset);
                expected.set(40 - offset);
                bits.set(30001 - offset);
                expected.set(30001 - offset);

                for (int upTo : new int[] { 37, 38, 20000, 30000, NO_MORE_DOCS }) {
                    postings.intoBitSet(upTo, bits, offset);
                    for (int doc = 37; doc < Math.min(upTo, 30000); doc += step) {
                        expected.set(doc - offset);
                    }
                    assertEquals(expected, bits);
                    int next = 37 + Math.max(0, (Math.min(upTo, 30000) - 37 + step - 1) / step) * step;
                    assertEquals(next >= 30000 ? NO_MORE_DOCS : next, postings.docID());
                }
                assertTrue(delegate.bitSetCalls > 0);
                assertEquals(0, delegate.scalarCalls);
                assertTrue(delegate.maxBitSetSpan <= 1 << 20);
            }
        }
    }

    /** Computing a cancellation chunk's endpoint must not overflow near Lucene's end-of-iteration sentinel. */
    public void testIntoBitSetNearNoMoreDocs() throws IOException {
        int first = NO_MORE_DOCS - 20000;
        BulkPostingsEnum delegate = new BulkPostingsEnum(first, NO_MORE_DOCS, 1, 256);
        FixedBitSet bits = new FixedBitSet(20000);
        PostingsEnum postings = wrap(delegate, new Cancellation());
        postings.intoBitSet(NO_MORE_DOCS, bits, first);
        assertEquals(20000, bits.cardinality());
        assertEquals(NO_MORE_DOCS, postings.docID());
        assertTrue(delegate.maxBitSetSpan <= 1 << 20);
        assertEquals(0, delegate.scalarCalls);
    }

    /** Cancellation after the first chunk must stop a large bitset fill before the second chunk begins. */
    public void testCancellationDuringIntoBitSet() throws IOException {
        Cancellation cancellation = new Cancellation();
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 3000000, 1, 256);
        delegate.afterBulk = () -> cancellation.cancelled = true;
        PostingsEnum postings = wrap(delegate, cancellation);
        FixedBitSet bits = new FixedBitSet(3000000);
        expectThrows(TaskCancelledException.class, () -> postings.intoBitSet(NO_MORE_DOCS, bits, 0));
        assertEquals(1 << 20, postings.docID());
        assertEquals(1 << 20, bits.cardinality());
        assertEquals(1, delegate.bitSetCalls);
        assertEquals(0, delegate.scalarCalls);
    }

    /**
     * Native batches must preserve document IDs, frequencies, exclusive bounds, and cursor position
     * for dense and sparse postings, including near the end-of-iteration sentinel.
     */
    public void testNextPostingsPreservesDocsFrequenciesAndCursor() throws IOException {
        for (int step : new int[] { 1, 17 }) {
            // Include a custom codec whose batches are larger than the cancellation interval.
            for (int batchSize : new int[] { 16, 256, 20000 }) {
                int first = NO_MORE_DOCS - 30000;
                BulkPostingsEnum delegate = new BulkPostingsEnum(first, NO_MORE_DOCS, step, batchSize);
                PostingsEnum postings = wrap(delegate, new Cancellation());
                DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
                int expected = first;
                for (int upTo : new int[] { first, first + 10000, NO_MORE_DOCS }) {
                    while (expected < upTo) {
                        postings.nextPostings(upTo, buffer);
                        assertTrue(buffer.size > 0);
                        assertTrue(buffer.size <= batchSize);
                        for (int i = 0; i < buffer.size; i++) {
                            assertTrue(expected < upTo);
                            assertEquals(expected, buffer.docs[i]);
                            assertEquals(1 + expected % 3, buffer.features[i], 0);
                            expected = (long) expected + step >= NO_MORE_DOCS ? NO_MORE_DOCS : expected + step;
                        }
                        assertEquals(expected, postings.docID());
                    }
                    buffer.size = 7;
                    postings.nextPostings(upTo, buffer);
                    assertEquals(0, buffer.size);
                    assertEquals(expected, postings.docID());
                }
                assertTrue(delegate.batchCalls > 0);
                assertEquals(0, delegate.scalarCalls);
            }
        }
    }

    /**
     * Sampling counts returned postings. A native batch may overshoot the budget, but the following
     * call must check cancellation before consuming any more postings.
     */
    public void testNextPostingsSamplesByWork() throws IOException {
        for (int batchSize : new int[] { 16, 256, 20000 }) {
            Cancellation cancellation = new Cancellation();
            BulkPostingsEnum delegate = new BulkPostingsEnum(0, 30000, 1, batchSize);
            PostingsEnum postings = wrap(delegate, cancellation);
            DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
            while (postings.docID() < 8192) {
                postings.nextPostings(NO_MORE_DOCS, buffer);
                assertEquals(1, cancellation.checks);
            }
            int expected = Math.max(8192, batchSize);
            assertEquals(expected, postings.docID());
            cancellation.cancelled = true;
            expectThrows(TaskCancelledException.class, () -> postings.nextPostings(NO_MORE_DOCS, buffer));
            assertEquals(expected, postings.docID());
        }
    }

    /** Bounding batches by document-ID distance would fragment sparse postings and lose native batching. */
    public void testSparseNextPostingsPreservesBatchSize() throws IOException {
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 10000000, 10000, 256);
        PostingsEnum postings = wrap(delegate, new Cancellation());
        DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
        postings.nextPostings(NO_MORE_DOCS, buffer);
        assertEquals(256, buffer.size);
        assertEquals(2550000, buffer.docs[255]);
        assertEquals(2560000, postings.docID());
        assertEquals(1, delegate.batchCalls);
        assertEquals(0, delegate.scalarCalls);
    }

    /** Compare wrapped and raw codec postings across bitset and batch operations on both dense and sparse terms. */
    public void testCodecPostings() throws IOException {
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
            for (int i = 0; i < 20000; i++) {
                Document document = new Document();
                document.add(new TextField("field", i % 17 == 0 ? "a a a b b" : "a a", Field.Store.NO));
                writer.addDocument(document);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = reader.leaves().getFirst().reader();
                for (String text : new String[] { "a", "b" }) {
                    Term term = new Term("field", text);
                    PostingsEnum expected = leaf.postings(term, PostingsEnum.FREQS);
                    PostingsEnum actual = wrap(leaf.postings(term, PostingsEnum.FREQS), new Cancellation());
                    assertEquals(expected.nextDoc(), actual.nextDoc());
                    assertEquals(expected.docIDRunEnd(), actual.docIDRunEnd());

                    FixedBitSet bits = new FixedBitSet(20000);
                    actual.intoBitSet(13000, bits, 0);
                    for (int doc = expected.docID(); doc < 13000; doc = expected.nextDoc()) {
                        assertTrue(bits.getAndClear(doc));
                    }
                    assertEquals(0, bits.cardinality());
                    assertEquals(expected.docID(), actual.docID());
                    assertEquals(expected.freq(), actual.freq());

                    DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
                    while (actual.docID() != NO_MORE_DOCS) {
                        actual.nextPostings(NO_MORE_DOCS, buffer);
                        assertTrue(buffer.size > 0);
                        for (int i = 0; i < buffer.size; i++) {
                            assertEquals(expected.docID(), buffer.docs[i]);
                            assertEquals(expected.freq(), buffer.features[i], 0);
                            expected.nextDoc();
                        }
                        assertEquals(expected.docID(), actual.docID());
                    }
                }
            }
        }
    }

    /** Scalar and batch operations must share a sampling budget so switching APIs cannot postpone cancellation. */
    public void testMixedScalarAndBatchCancellation() throws IOException {
        Cancellation cancellation = new Cancellation();
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 30000, 1, 256);
        PostingsEnum postings = wrap(delegate, cancellation);
        assertEquals(1, postings.nextDoc());
        assertEquals(2, postings.advance(2));
        postings.nextPostings(NO_MORE_DOCS, new DocAndFloatFeatureBuffer());
        assertEquals(258, postings.docID());
        assertEquals(1, cancellation.checks);
        while (postings.docID() < 8192) {
            postings.nextPostings(NO_MORE_DOCS, new DocAndFloatFeatureBuffer());
            assertEquals(1, cancellation.checks);
        }
        cancellation.cancelled = true;
        expectThrows(TaskCancelledException.class, postings::nextDoc);
        // The final native batch may overshoot the remaining budget.
        assertEquals(8194, postings.docID());
        assertEquals(2, delegate.scalarCalls);
    }

    /** Even short bitset fills must check cancellation at the next call, regardless of the scalar sampling counter. */
    public void testCancellationBetweenBitSetCalls() throws IOException {
        Cancellation cancellation = new Cancellation();
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 30000, 1, 256);
        PostingsEnum postings = wrap(delegate, cancellation);
        postings.nextDoc();
        delegate.afterBulk = () -> cancellation.cancelled = true;
        FixedBitSet bits = new FixedBitSet(30000);
        postings.intoBitSet(10, bits, 0);
        expectThrows(TaskCancelledException.class, () -> postings.intoBitSet(20, bits, 0));
        assertEquals(10, postings.docID());
        assertEquals(1, delegate.bitSetCalls);
    }

    /** Preserve the first-call check and 8192-call interval across interleaved nextDoc and advance operations. */
    public void testScalarSampling() throws IOException {
        Cancellation cancellation = new Cancellation();
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 30000, 1, 256);
        PostingsEnum postings = wrap(delegate, cancellation);
        assertEquals(1, postings.nextDoc());
        cancellation.cancelled = true;
        for (int doc = 2; doc <= 8192; doc++) {
            assertEquals(doc, doc % 2 == 0 ? postings.advance(doc) : postings.nextDoc());
        }
        assertEquals(1, cancellation.checks);
        expectThrows(TaskCancelledException.class, postings::nextDoc);
        assertEquals(8192, postings.docID());
        assertEquals(8192, delegate.scalarCalls);
    }

    /** An already-cancelled query must stop before entering either codec bulk operation. */
    public void testCancelledBeforeBulkWork() throws IOException {
        Cancellation cancellation = new Cancellation();
        cancellation.cancelled = true;
        BulkPostingsEnum delegate = new BulkPostingsEnum(0, 30000, 1, 256);
        PostingsEnum postings = wrap(delegate, cancellation);
        expectThrows(TaskCancelledException.class, () -> postings.intoBitSet(NO_MORE_DOCS, new FixedBitSet(30000), 0));
        expectThrows(TaskCancelledException.class, () -> postings.nextPostings(NO_MORE_DOCS, new DocAndFloatFeatureBuffer()));
        assertEquals(0, delegate.bitSetCalls);
        assertEquals(0, delegate.batchCalls);
        assertEquals(0, postings.docID());
    }

    private static PostingsEnum wrap(PostingsEnum delegate, Cancellation cancellation) {
        return new ExitableDirectoryReader.ExitablePostingsEnum(delegate, cancellation);
    }

    private static class Cancellation implements ExitableDirectoryReader.QueryCancellation {
        boolean cancelled;
        int checks;

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void checkCancelled() {
            checks++;
            if (cancelled) {
                throw new TaskCancelledException("cancelled");
            }
        }
    }

    /** Already positioned postings with observable bulk methods, independent of a codec's block layout. */
    private static class BulkPostingsEnum extends PostingsEnum {
        private int doc;
        private final int end;
        private final int step;
        private final int batchSize;
        int scalarCalls;
        int bitSetCalls;
        int batchCalls;
        int maxBitSetSpan;
        Runnable afterBulk = () -> {};

        BulkPostingsEnum(int first, int end, int step, int batchSize) {
            this.doc = first;
            this.end = end;
            this.step = step;
            this.batchSize = batchSize;
        }

        private void move() {
            doc = (long) doc + step >= end ? NO_MORE_DOCS : doc + step;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            scalarCalls++;
            move();
            return doc;
        }

        @Override
        public int advance(int target) {
            scalarCalls++;
            long next = doc + ((long) target - doc + step - 1) / step * step;
            return doc = next >= end ? NO_MORE_DOCS : (int) next;
        }

        @Override
        public int docIDRunEnd() {
            return step == 1 ? end : doc + 1;
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bits, int offset) {
            bitSetCalls++;
            maxBitSetSpan = Math.max(maxBitSetSpan, upTo - doc);
            while (doc < upTo) {
                bits.set(doc - offset);
                move();
            }
            afterBulk.run();
        }

        @Override
        public void nextPostings(int upTo, DocAndFloatFeatureBuffer buffer) {
            batchCalls++;
            buffer.growNoCopy(batchSize);
            buffer.size = 0;
            while (doc < upTo && buffer.size < batchSize) {
                buffer.docs[buffer.size] = doc;
                buffer.features[buffer.size++] = freq();
                move();
            }
            afterBulk.run();
        }

        @Override
        public long cost() {
            return end / step;
        }

        @Override
        public int freq() {
            return 1 + doc % 3;
        }

        @Override
        public int nextPosition() {
            return -1;
        }

        @Override
        public int startOffset() {
            return -1;
        }

        @Override
        public int endOffset() {
            return -1;
        }

        @Override
        public BytesRef getPayload() {
            return null;
        }
    }
}
