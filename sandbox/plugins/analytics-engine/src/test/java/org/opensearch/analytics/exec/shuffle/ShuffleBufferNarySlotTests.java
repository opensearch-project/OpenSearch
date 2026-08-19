/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.shuffle;

import org.opensearch.analytics.spi.CloseableIterator;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests the N-ARY (slot-keyed) shuffle buffer: a consumer with more than two input streams. The binary
 * left/right path is covered by {@link ShuffleBufferManagerTests}; this class pins the properties that
 * only an N-input consumer can exercise — per-slot isolation, per-slot completion latches, per-slot spill
 * files, and the invariant that {@code awaitReady} waits for EVERY declared slot.
 */
public class ShuffleBufferNarySlotTests extends OpenSearchTestCase {

    public void testThreeSlotsAccumulateIndependently() {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.addData("in0", new byte[] { 1 });
        buffer.addData("in1", new byte[] { 2, 2 });
        buffer.addData("in2", new byte[] { 3, 3, 3 });

        // Cross-slot leakage would silently feed one join input's rows to another.
        assertEquals(List.of(1), firstBytes(buffer.getData("in0")));
        assertEquals(List.of(2), firstBytes(buffer.getData("in1")));
        assertEquals(List.of(3), firstBytes(buffer.getData("in2")));
        assertEquals("byte accounting is buffer-wide across slots", 6L, buffer.getCurrentBytes());
        assertEquals(java.util.Set.of("in0", "in1", "in2"), buffer.getSlots());
    }

    public void testAwaitReadyWaitsForEverySlot() throws Exception {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        bySlot.put("in0", 1);
        bySlot.put("in1", 1);
        bySlot.put("in2", 1);
        buffer.setExpectedSenders(bySlot);

        buffer.senderDone("in0");
        buffer.senderDone("in1");
        // in2 outstanding — a buffer that returned ready here would let the worker drain a partition
        // whose third input has not arrived, producing silently-missing rows.
        assertFalse("must not be ready while one slot is outstanding", buffer.awaitReady(50));

        buffer.senderDone("in2");
        assertTrue("ready once every declared slot is complete", buffer.awaitReady(1000));
    }

    public void testMultipleSendersPerSlot() throws Exception {
        // Each slot's expected count is independent — a slot fed by 3 shard producers must not be
        // considered complete after 1.
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        bySlot.put("in0", 3);
        bySlot.put("in1", 1);
        buffer.setExpectedSenders(bySlot);

        buffer.senderDone("in0");
        buffer.senderDone("in1");
        assertFalse("in0 still awaits 2 more senders", buffer.awaitReady(50));
        buffer.senderDone("in0");
        buffer.senderDone("in0");
        assertTrue(buffer.awaitReady(1000));
        assertEquals(3, buffer.getDoneCount("in0"));
        assertEquals(3, buffer.getExpectedSenders("in0"));
    }

    public void testUndeclaredSlotDoesNotBlockAwaitReady() throws Exception {
        // A producer payload can arrive for a slot this consumer never declares (a stray/late RPC),
        // creating the slot with expectedSenders=-1. awaitReady must ignore it rather than hang: -1 is a
        // target no sender count can ever reach.
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.setExpectedSenders(Map.of("in0", 1));
        buffer.addData("in-stray", new byte[] { 9 });

        buffer.senderDone("in0");
        assertTrue("an undeclared slot must not extend the wait", buffer.awaitReady(1000));
    }

    public void testSetExpectedSendersIgnoresNegativeCounts() throws Exception {
        // Negative means "leave unchanged" (the single-slot agg path's unused right side).
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        Map<String, Integer> bySlot = new LinkedHashMap<>();
        bySlot.put("in0", 1);
        bySlot.put("in1", -1);
        buffer.setExpectedSenders(bySlot);

        assertEquals("negative-count slot is not declared", -1, buffer.getExpectedSenders("in1"));
        buffer.senderDone("in0");
        assertTrue(buffer.awaitReady(1000));
    }

    public void testEmptySlotDrainsAsZeroChunks() {
        // A partition where a slot received nothing must drain as an empty stream, not NPE — the worker
        // still registers a resolvable table so the plan binds and the join yields 0 rows for it.
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        buffer.addData("in0", new byte[] { 1 });

        assertTrue(buffer.getData("in1").isEmpty());
        try (CloseableIterator<byte[]> it = buffer.drain("in1")) {
            assertFalse(it.hasNext());
        }
    }

    public void testEachSlotSpillsToItsOwnFile() throws Exception {
        // Per-slot spill files must not collide: a shared name would interleave two inputs' chunks into
        // one stream, so the consumer would read another slot's rows.
        Path spillRoot = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setSpillConfig(true, spillRoot, Long.MAX_VALUE);
        // A tiny per-query budget forces every admit past the first to spill.
        mgr.setBudgets(Long.MAX_VALUE, 4L);

        for (String slot : List.of("in0", "in1", "in2")) {
            assertEquals(ShuffleBufferManager.AdmitResult.ACCEPTED, mgr.tryAdmit("q-nary", 7, 0, slot, new byte[] { 1, 2, 3 }));
            assertEquals(ShuffleBufferManager.AdmitResult.ACCEPTED, mgr.tryAdmit("q-nary", 7, 0, slot, new byte[] { 4, 5, 6 }));
        }

        Path queryDir = spillRoot.resolve("q-nary");
        assertTrue("spill dir must exist once the budget forced eviction", Files.isDirectory(queryDir));
        try (var files = Files.list(queryDir)) {
            List<String> names = files.map(p -> p.getFileName().toString()).sorted().toList();
            // One file per slot that spilled, each naming its own slot.
            assertTrue("expected per-slot spill files, got " + names, names.stream().anyMatch(n -> n.endsWith("-in0.spill")));
            assertTrue("expected per-slot spill files, got " + names, names.stream().anyMatch(n -> n.endsWith("-in1.spill")));
        }

        // Read-back must reconstruct each slot's own chunks (spilled head then in-memory tail), with no
        // cross-slot contamination.
        ShuffleBufferManager.ShuffleBuffer buffer = mgr.getBuffer("q-nary", 7, 0);
        for (String slot : List.of("in0", "in1", "in2")) {
            List<byte[]> chunks = new ArrayList<>();
            try (CloseableIterator<byte[]> it = buffer.drain(slot)) {
                while (it.hasNext()) {
                    chunks.add(it.next());
                }
            }
            assertEquals("slot " + slot + " must yield both its chunks", 2, chunks.size());
            assertEquals("slot " + slot + " chunk order is arrival order", 1, chunks.get(0)[0]);
            assertEquals(4, chunks.get(1)[0]);
        }
        mgr.clearForQuery("q-nary");
    }

    public void testClearForQueryReleasesAllSlots() {
        Path spillRoot = createTempDir();
        ShuffleBufferManager mgr = new ShuffleBufferManager();
        mgr.setSpillConfig(true, spillRoot, Long.MAX_VALUE);
        mgr.setBudgets(Long.MAX_VALUE, 4L);
        for (String slot : List.of("in0", "in1", "in2")) {
            mgr.tryAdmit("q-clear", 3, 0, slot, new byte[] { 1, 2, 3 });
            mgr.tryAdmit("q-clear", 3, 0, slot, new byte[] { 4, 5, 6 });
        }

        assertEquals(1, mgr.clearForQuery("q-clear"));
        assertEquals("every slot's on-heap reservation is released", 0L, mgr.getQueryBytes("q-clear"));
        assertEquals("node total is released", 0L, mgr.getTotalBytes());
        assertEquals("every slot's disk bytes are released", 0L, mgr.getSpilledTotalBytes());
        assertFalse("per-query spill dir is swept", Files.isDirectory(spillRoot.resolve("q-clear")));
    }

    public void testRejectsUnsafeSlotLabel() {
        ShuffleBufferManager.ShuffleBuffer buffer = new ShuffleBufferManager.ShuffleBuffer();
        // Validated at every entry point so a hostile label can never reach spillFilePath().
        expectThrows(IllegalArgumentException.class, () -> buffer.addData("../escape", new byte[] { 1 }));
        expectThrows(IllegalArgumentException.class, () -> buffer.senderDone("a/b"));
    }

    /** First byte of each chunk, as ints — a compact way to assert chunk identity and order. */
    private static List<Integer> firstBytes(List<byte[]> chunks) {
        return chunks.stream().map(c -> (int) c[0]).toList();
    }
}
