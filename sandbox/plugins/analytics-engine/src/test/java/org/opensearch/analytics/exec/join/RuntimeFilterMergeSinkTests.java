/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the coordinator-side union of per-shard Bloom contributions, which happens incrementally
 * in the sink so no reservation scales with shard count.
 *
 * <p>The union is where a mistake would silently lose rows rather than performance, so the cases below are
 * about exactly that: a bit set on any shard must survive, and anything that cannot be unioned soundly must
 * produce no filter at all.
 */
public class RuntimeFilterMergeSinkTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testUnionsEveryShardsBits() throws Exception {
        // A key hashes to a bit on whichever shard holds it, so the union has to be an OR: keeping only one
        // shard's bits would report "absent" for keys another shard holds, which drops matching probe rows.
        byte[] ipc = oneBatch(new byte[] { 0b0000_0001, 0b0000_0000 }, new byte[] { 0b0000_0000, (byte) 0b1000_0000 });
        assertArrayEquals(new byte[] { 0b0000_0001, (byte) 0b1000_0000 }, mergeThrough(ipc));
    }

    public void testUnionsAcrossBatches() throws Exception {
        byte[] ipc = bitsetStream(List.of(new byte[] { 0b0000_0010 }), List.of(new byte[] { 0b0000_0100 }));
        assertArrayEquals(new byte[] { 0b0000_0110 }, mergeThrough(ipc));
    }

    public void testSkipsShardsThatContributedNothing() throws Exception {
        // A shard whose build fragment matched no rows emits null, which is not the same as an empty filter:
        // it must not reset or shrink what the other shards found.
        byte[] ipc = oneBatch(new byte[] { 0b0000_1000 }, null);
        assertArrayEquals(new byte[] { 0b0000_1000 }, mergeThrough(ipc));
    }

    public void testAllShardsContributingNothingYieldsNoPayload() throws Exception {
        assertNull(mergeThrough(oneBatch(null, null)));
        assertNull("no rows at all", mergeThrough(oneBatch()));
    }

    public void testRefusesContributionsOfDifferentSizes() throws Exception {
        // Two differently sized SBBFs index blocks differently, so a byte-wise combination of them would
        // report absent for keys that are present — the one failure mode that loses rows. Refusing is what
        // keeps that unreachable even if the size invariant is ever broken upstream.
        byte[] ipc = oneBatch(new byte[] { 1, 2, 3, 4 }, new byte[] { 1, 2 });
        assertNull(mergeThrough(ipc));
    }

    public void testRejectsANonBitsetColumn() throws Exception {
        // The pre-pass aggregate's output is VARBINARY. Anything else means this is not a pre-pass capture,
        // and reinterpreting it as a bitset would install a filter over unrelated bytes.
        Schema schema = new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            BigIntVector vector = (BigIntVector) root.getVector("k");
            byte[] ipc = writeOneBatch(root, () -> {
                vector.allocateNew(1);
                vector.setSafe(0, 7L);
                return 1;
            });
            assertNull(mergeThrough(ipc));
        }
    }

    public void testAThrowingFeedLeavesTheBatchToTheCaller() {
        // ExchangeSink.feed is all-or-nothing: on a throw the caller still owns and closes the batch, so
        // closing here too (as a `finally` would) double-releases Arrow buffers. Mocked because the real
        // trigger is an allocation failure that a well-formed batch cannot provoke reliably.
        VectorSchemaRoot batch = mock(VectorSchemaRoot.class);
        RuntimeException boom = new RuntimeException("read failed");
        when(batch.getFieldVectors()).thenThrow(boom);

        RuntimeFilterMergeSink sink = new RuntimeFilterMergeSink(0);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> sink.feed(batch));

        assertSame("the failure propagates rather than being swallowed", boom, thrown);
        verify(batch, never()).close();
    }

    public void testASuccessfulFeedReleasesTheBatch() throws Exception {
        // The other half of the contract: on normal return the bytes are copied into the accumulator, so the
        // batch must be released immediately rather than held for a later merge pass.
        byte[] ipc = oneBatch(new byte[] { 1, 0, 0, 0 });
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertTrue(reader.loadNextBatch());
            VectorSchemaRoot batch = copyOf(root);
            long allocatedBefore = allocator.getAllocatedMemory();

            new RuntimeFilterMergeSink(0).feed(batch);

            assertTrue(
                "the batch's buffers are released on the success path",
                allocator.getAllocatedMemory() < allocatedBefore || allocatedBefore == 0
            );
        }
    }

    public void testInvalidateDiscardsAPartialUnion() throws Exception {
        // Shards A and B report, shard C's task fails. Their union is well formed and correctly sized, so no
        // per-contribution check can see that a shard's keys are missing — and a Bloom missing keys reports
        // ABSENT for keys that are present, making the probe reject rows that belong in the result.
        RuntimeFilterMergeSink sink = new RuntimeFilterMergeSink(0);
        feedInto(sink, oneBatch(new byte[] { 1, 0, 0, 0 }));
        assertNotNull("a contribution was accumulated", sink.mergedBitset());

        sink.invalidate();

        assertNull("a partial union must not be publishable", sink.mergedBitset());
    }

    public void testAContributionArrivingAfterInvalidateIsIgnored() throws Exception {
        // Stage termination and shard responses race, so a contribution can land after the stage has been
        // observed as failed. It must not resurrect the filter.
        RuntimeFilterMergeSink sink = new RuntimeFilterMergeSink(0);
        sink.invalidate();

        feedInto(sink, oneBatch(new byte[] { 1, 0, 0, 0 }));

        assertNull("a late contribution must not revive an abandoned filter", sink.mergedBitset());
    }

    /** Feeds every batch of an IPC stream into {@code sink}, transferring ownership as the contract requires. */
    private void feedInto(RuntimeFilterMergeSink sink, byte[] ipc) throws Exception {
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                sink.feed(copyOf(root));
            }
        }
    }

    public void testUnreadableBytesYieldNoPayload() throws Exception {
        assertNull(mergeThrough(new byte[] { 1, 2, 3 }));
        assertNull(mergeThrough(new byte[0]));
        assertNull(mergeThrough(null));
    }

    /**
     * Feeds an IPC stream through the sink one batch at a time, returning its union — so the cases below
     * keep asserting the same semantics they were written for, now against the incremental implementation.
     * Unreadable bytes yield null, matching what the sink does when it is never fed.
     */
    private byte[] mergeThrough(byte[] ipc) throws Exception {
        RuntimeFilterMergeSink sink = new RuntimeFilterMergeSink(0);
        if (ipc == null || ipc.length == 0) {
            return sink.mergedBitset();
        }
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                // The sink takes ownership on feed, so hand it a transferred copy and keep the reader's root.
                try (VectorSchemaRoot batch = copyOf(root)) {
                    sink.feed(copyOf(root));
                }
            }
        } catch (java.io.IOException | IllegalArgumentException e) {
            return null;
        }
        return sink.mergedBitset();
    }

    /** A standalone batch the sink may close, independent of the reader's root. */
    private VectorSchemaRoot copyOf(VectorSchemaRoot root) {
        VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), allocator);
        copy.allocateNew();
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            FieldVector src = root.getVector(i);
            FieldVector dst = copy.getVector(i);
            for (int row = 0; row < root.getRowCount(); row++) {
                dst.copyFromSafe(row, row, src);
            }
        }
        copy.setRowCount(root.getRowCount());
        return copy;
    }

    // ── Fixtures ─────────────────────────────────────────────────────────

    /** One batch holding one row per argument; a null argument is a shard that contributed nothing. */
    private byte[] oneBatch(byte[]... rows) throws Exception {
        return bitsetStream(Arrays.asList(rows));
    }

    @SafeVarargs
    private byte[] bitsetStream(List<byte[]>... batches) throws Exception {
        Schema schema = new Schema(List.of(new Field("bitset", FieldType.nullable(new ArrowType.Binary()), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            VarBinaryVector vector = (VarBinaryVector) root.getVector("bitset");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                for (List<byte[]> batch : batches) {
                    for (FieldVector v : root.getFieldVectors()) {
                        v.clear();
                    }
                    vector.allocateNew(batch.size());
                    for (int row = 0; row < batch.size(); row++) {
                        if (batch.get(row) == null) {
                            vector.setNull(row);
                        } else {
                            vector.setSafe(row, batch.get(row));
                        }
                    }
                    root.setRowCount(batch.size());
                    writer.writeBatch();
                }
                writer.end();
            }
            return out.toByteArray();
        }
    }

    private byte[] writeOneBatch(VectorSchemaRoot root, BatchFiller filler) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            root.setRowCount(filler.fill());
            writer.writeBatch();
            writer.end();
        }
        return out.toByteArray();
    }

    private interface BatchFiller {
        int fill();
    }
}
