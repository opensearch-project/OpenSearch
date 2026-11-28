/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.exec.shuffle.ShuffleCompression;
import org.opensearch.analytics.spi.ShuffleSender;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests for {@link DatafusionPartitionedSink}'s per-partition payload contract.
 *
 * <p>The consumer ({@link ShuffleScanHandler}) derives the schema of the streaming table it
 * registers from the FIRST chunk it drains for its partition. A partition that no producer row
 * hashed into must therefore still receive one schema-carrying chunk, otherwise the consumer has
 * nothing to register the named scan from and the whole fragment fails.
 */
// The native library is loaded process-wide by NativeBridge; its threads outlive the test class.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class DatafusionPartitionedSinkTests extends OpenSearchTestCase {

    /** One {@link ShuffleSender#send} call, as the consumer's transport action would see it. */
    private record Sent(int partition, byte[] data, boolean isLast) {
    }

    /** Records every shipped payload instead of hitting the transport. Completes sends inline. */
    private static final class RecordingShuffleSender implements ShuffleSender {
        private final List<Sent> sends = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void send(String targetWorkerNodeId, int partitionIndex, byte[] data, boolean isLast, ActionListener<Void> listener) {
            sends.add(new Sent(partitionIndex, data, isLast));
            listener.onResponse(null);
        }

        List<Sent> forPartition(int partition) {
            return sends.stream().filter(s -> s.partition() == partition).collect(Collectors.toList());
        }
    }

    private static Schema keySchema() {
        return new Schema(List.of(new Field("k", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    }

    /** A batch of {@code rowCount} rows that all carry the same key, so they all hash to one partition. */
    private static VectorSchemaRoot singleKeyBatch(RootAllocator alloc, long key, int rowCount) {
        VectorSchemaRoot root = VectorSchemaRoot.create(keySchema(), alloc);
        BigIntVector v = (BigIntVector) root.getVector("k");
        v.allocateNew(rowCount);
        for (int i = 0; i < rowCount; i++) {
            v.setSafe(i, key);
        }
        v.setValueCount(rowCount);
        root.setRowCount(rowCount);
        return root;
    }

    private static DatafusionPartitionedSink newSink(RootAllocator alloc, int partitionCount, RecordingShuffleSender sender) {
        List<String> targets = IntStream.range(0, partitionCount).mapToObj(p -> "node-" + p).collect(Collectors.toList());
        return new DatafusionPartitionedSink(
            alloc,
            List.of(0),
            partitionCount,
            targets,
            sender,
            "test/stage=1/left",
            ShuffleCompression.Config.DISABLED
        );
    }

    /** A zero-row batch carrying only a schema — what {@code DatafusionResultStream} synthesises for
     *  an empty native stream, and the only thing a producer with no rows ever feeds this sink. */
    private static VectorSchemaRoot emptyBatch(RootAllocator alloc) {
        VectorSchemaRoot root = VectorSchemaRoot.create(keySchema(), alloc);
        root.setRowCount(0);
        return root;
    }

    /**
     * Every partition — including the ones no row hashed into — must receive at least one chunk the
     * consumer can read a schema out of. Holds because the native partitioner emits an empty
     * {@code RecordBatch} (not a null FFI pair) for every partition no row hashed into.
     */
    public void testEveryPartitionReceivesASchemaCarryingChunk() throws Exception {
        final int partitionCount = 4;
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            RecordingShuffleSender sender = new RecordingShuffleSender();
            DatafusionPartitionedSink sink = newSink(alloc, partitionCount, sender);

            // A single distinct key hashes into one partition; the rest receive an empty batch.
            sink.feed(singleKeyBatch(alloc, 7L, 64));
            sink.close();

            for (int p = 0; p < partitionCount; p++) {
                List<Sent> sends = sender.forPartition(p);
                assertEquals("partition " + p + " must get exactly one isLast send", 1, sends.stream().filter(Sent::isLast).count());

                byte[] firstPayload = sends.stream().map(Sent::data).filter(d -> d != null && d.length > 0).findFirst().orElse(null);
                assertNotNull(
                    "partition " + p + " received no schema-carrying chunk; the consumer cannot register its named scan",
                    firstPayload
                );
                assertEquals("partition " + p + " chunk must carry the fed schema", keySchema(), ArrowSchemaIpc.fromBytes(firstPayload));
            }
        }
    }

    /**
     * Regression test for the hash-shuffle join failing outright when one side has no rows.
     *
     * <p>A producer whose engine yields nothing feeds this sink exactly one zero-row,
     * schema-bearing batch. The sink used to drop that batch before recording anything and close
     * every partition with a zero-length {@code isLast} marker, which the buffer manager does not
     * even store — so the join-side consumer drained zero chunks, had no producer plan to re-lower a
     * schema from, and handed the native side an empty IPC blob. The fragment then failed with
     * {@code schema_from_ipc_bytes: Ipc error: Expected schema message, found empty stream}. Now the
     * schema rides the final marker so the consumer can register a bindable zero-row table.
     */
    public void testZeroRowFeedShipsSchemaOnFinalMarkerToEveryPartition() throws Exception {
        final int partitionCount = 4;
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            RecordingShuffleSender sender = new RecordingShuffleSender();
            DatafusionPartitionedSink sink = newSink(alloc, partitionCount, sender);

            sink.feed(emptyBatch(alloc));
            sink.close();

            for (int p = 0; p < partitionCount; p++) {
                List<Sent> sends = sender.forPartition(p);
                assertEquals("partition " + p + " must get exactly one send (the final marker)", 1, sends.size());
                Sent marker = sends.get(0);
                assertTrue("partition " + p + "'s only send must be the isLast marker", marker.isLast());
                assertTrue(
                    "partition " + p + "'s marker must carry a schema payload, not an empty one",
                    marker.data() != null && marker.data().length > 0
                );
                assertEquals("partition " + p + " marker must carry the fed schema", keySchema(), ArrowSchemaIpc.fromBytes(marker.data()));
            }
        }
    }

    /**
     * A sink that never saw a batch never saw a schema either, so it can only ship bare markers.
     * Pins that boundary: the sink does not invent a schema, and the consumer's diagnostic for this
     * case (no chunks AND no schema anywhere) stays reachable.
     */
    public void testSinkThatFedNothingShipsBareMarkers() {
        final int partitionCount = 3;
        try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
            RecordingShuffleSender sender = new RecordingShuffleSender();
            DatafusionPartitionedSink sink = newSink(alloc, partitionCount, sender);
            sink.close();

            for (int p = 0; p < partitionCount; p++) {
                List<Sent> sends = sender.forPartition(p);
                assertEquals("partition " + p + " must get exactly one send", 1, sends.size());
                assertTrue("partition " + p + "'s only send must be the isLast marker", sends.get(0).isLast());
                assertEquals("partition " + p + "'s marker must be empty", 0, sends.get(0).data().length);
            }
        }
    }
}
