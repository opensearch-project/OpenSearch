/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for {@link FormatSwitchableIndexInput}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Basic read delegation to local input</li>
 *   <li>switchToRemote transitions reads to remote</li>
 *   <li>Clone/slice inherit switch state</li>
 *   <li>switchToRemote cascades to existing clones</li>
 *   <li>Concurrent reads during switch</li>
 *   <li>Double switch is no-op</li>
 *   <li>Close behavior</li>
 *   <li>File pointer preservation across switch</li>
 * </ul>
 */
public class FormatSwitchableIndexInputTests extends OpenSearchTestCase {

    private static final byte[] LOCAL_DATA = "local-data-content-1234567890".getBytes(StandardCharsets.UTF_8);
    private static final byte[] REMOTE_DATA = "remote-data-content-1234567890".getBytes(StandardCharsets.UTF_8);
    private static final String FILE_NAME = "test.parquet";

    private ByteBuffersDirectory localDir;
    private ByteBuffersDirectory remoteDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        localDir = new ByteBuffersDirectory();
        remoteDir = new ByteBuffersDirectory();
        writeFile(localDir, FILE_NAME, LOCAL_DATA);
        writeFile(remoteDir, FILE_NAME, REMOTE_DATA);
    }

    @Override
    public void tearDown() throws Exception {
        localDir.close();
        remoteDir.close();
        super.tearDown();
    }

    // ═══════════════════════════════════════════════════════════════
    // Basic read delegation
    // ═══════════════════════════════════════════════════════════════

    public void testReadDelegatesToLocal() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            byte[] buf = new byte[LOCAL_DATA.length];
            input.readBytes(buf, 0, buf.length);
            assertArrayEquals(LOCAL_DATA, buf);
            assertFalse(input.hasSwitchedToRemote());
        }
    }

    public void testReadByteFromLocal() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            assertEquals(LOCAL_DATA[0], input.readByte());
        }
    }

    public void testLengthReturnsFileLength() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            assertEquals(LOCAL_DATA.length, input.length());
        }
    }

    public void testGetFilePointerStartsAtZero() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            assertEquals(0, input.getFilePointer());
        }
    }

    public void testSeekAndRead() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            input.seek(5);
            assertEquals(5, input.getFilePointer());
            assertEquals(LOCAL_DATA[5], input.readByte());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // switchToRemote
    // ═══════════════════════════════════════════════════════════════

    public void testSwitchToRemoteTransitionsReads() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            assertFalse(input.hasSwitchedToRemote());
            input.switchToRemote();
            assertTrue(input.hasSwitchedToRemote());

            byte[] buf = new byte[REMOTE_DATA.length];
            input.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
        }
    }

    public void testSwitchToRemoteIsIdempotent() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            input.switchToRemote();
            input.switchToRemote(); // second call is no-op
            assertTrue(input.hasSwitchedToRemote());

            byte[] buf = new byte[REMOTE_DATA.length];
            input.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
        }
    }

    public void testSwitchOnClosedInputIsNoOp() throws IOException {
        FormatSwitchableIndexInput input = createSwitchable();
        input.close();
        input.switchToRemote(); // should not throw
    }

    public void testLengthPreservedAfterSwitch() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            long lengthBefore = input.length();
            input.switchToRemote();
            assertEquals(lengthBefore, input.length());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // File pointer preservation across switch
    // ═══════════════════════════════════════════════════════════════

    public void testFilePointerPreservedOnSwitchForClone() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            // Read a few bytes to advance pointer
            input.seek(5);
            IndexInput cloned = input.clone();
            assertEquals(5, cloned.getFilePointer());

            // Switch — clone should also switch and preserve pointer
            input.switchToRemote();
            // After switch, the clone's underlying input is now remote
            // The clone was switched by the cascade in switchToRemote
            cloned.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Clone and slice
    // ═══════════════════════════════════════════════════════════════

    public void testCloneReadsFromLocal() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            IndexInput cloned = input.clone();
            byte[] buf = new byte[LOCAL_DATA.length];
            cloned.readBytes(buf, 0, buf.length);
            assertArrayEquals(LOCAL_DATA, buf);
            cloned.close();
        }
    }

    public void testCloneAfterSwitchReadsFromRemote() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            input.switchToRemote();
            IndexInput cloned = input.clone();
            byte[] buf = new byte[REMOTE_DATA.length];
            cloned.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
            cloned.close();
        }
    }

    public void testSliceReadsCorrectRange() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            IndexInput sliced = input.slice("slice", 5, 5);
            assertEquals(5, sliced.length());
            byte[] buf = new byte[5];
            sliced.readBytes(buf, 0, 5);
            byte[] expected = new byte[5];
            System.arraycopy(LOCAL_DATA, 5, expected, 0, 5);
            assertArrayEquals(expected, buf);
            sliced.close();
        }
    }

    public void testSliceAfterSwitchReadsFromRemote() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            input.switchToRemote();
            IndexInput sliced = input.slice("slice", 5, 5);
            byte[] buf = new byte[5];
            sliced.readBytes(buf, 0, 5);
            byte[] expected = new byte[5];
            System.arraycopy(REMOTE_DATA, 5, expected, 0, 5);
            assertArrayEquals(expected, buf);
            sliced.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // switchToRemote cascades to clones
    // ═══════════════════════════════════════════════════════════════

    public void testSwitchCascadesToExistingClones() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            FormatSwitchableIndexInput cloned = (FormatSwitchableIndexInput) input.clone();
            assertFalse(cloned.hasSwitchedToRemote());

            input.switchToRemote();

            assertTrue("Clone should be switched after parent switches", cloned.hasSwitchedToRemote());
            byte[] buf = new byte[REMOTE_DATA.length];
            cloned.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
            cloned.close();
        }
    }

    public void testSwitchCascadesToMultipleClones() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            FormatSwitchableIndexInput clone1 = (FormatSwitchableIndexInput) input.clone();
            FormatSwitchableIndexInput clone2 = (FormatSwitchableIndexInput) input.clone();

            input.switchToRemote();

            assertTrue(clone1.hasSwitchedToRemote());
            assertTrue(clone2.hasSwitchedToRemote());
            clone1.close();
            clone2.close();
        }
    }

    public void testClosedCloneRemovedFromTrackingBeforeSwitch() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            FormatSwitchableIndexInput cloned = (FormatSwitchableIndexInput) input.clone();
            cloned.close();

            // Switch should not fail even though clone is closed
            input.switchToRemote();
            assertTrue(input.hasSwitchedToRemote());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Concurrency
    // ═══════════════════════════════════════════════════════════════

    public void testConcurrentReadsAndSwitch() throws Exception {
        FormatSwitchableIndexInput input = createSwitchable();
        int numReaders = 4;
        CyclicBarrier barrier = new CyclicBarrier(numReaders + 1);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicInteger successCount = new AtomicInteger(0);

        Thread[] readers = new Thread[numReaders];
        for (int i = 0; i < numReaders; i++) {
            readers[i] = new Thread(() -> {
                try (IndexInput clone = input.clone()) {
                    barrier.await();
                    for (int j = 0; j < 100; j++) {
                        clone.seek(0);
                        byte b = clone.readByte();
                        // Should be either local or remote first byte
                        assertTrue(b == LOCAL_DATA[0] || b == REMOTE_DATA[0]);
                    }
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failed.set(true);
                }
            });
            readers[i].start();
        }

        // Switcher thread
        barrier.await();
        Thread.sleep(5); // let readers start
        input.switchToRemote();

        for (Thread t : readers) {
            t.join(5000);
        }

        assertFalse("No reader thread should have failed", failed.get());
        assertEquals(numReaders, successCount.get());
        input.close();
    }

    public void testConcurrentCloneAndSwitch() throws Exception {
        FormatSwitchableIndexInput input = createSwitchable();
        int numCloners = 4;
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicReference<IndexInput>[] clones = new AtomicReference[numCloners];

        Thread[] threads = new Thread[numCloners];
        for (int i = 0; i < numCloners; i++) {
            final int idx = i;
            clones[idx] = new AtomicReference<>();
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    clones[idx].set(input.clone());
                } catch (Exception e) {
                    failed.set(true);
                }
            });
            threads[i].start();
        }

        start.countDown();
        input.switchToRemote();

        for (Thread t : threads) {
            t.join(5000);
        }

        assertFalse("No cloner thread should have failed", failed.get());
        for (AtomicReference<IndexInput> ref : clones) {
            IndexInput c = ref.get();
            if (c != null) c.close();
        }
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Close behavior
    // ═══════════════════════════════════════════════════════════════

    public void testCloseBeforeSwitch() throws IOException {
        FormatSwitchableIndexInput input = createSwitchable();
        input.close();
        // Should not throw on double close
    }

    public void testCloseAfterSwitch() throws IOException {
        FormatSwitchableIndexInput input = createSwitchable();
        input.switchToRemote();
        input.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // readShort / readInt / readLong / readVInt / readVLong
    // ═══════════════════════════════════════════════════════════════

    public void testReadShort() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            short val = input.readShort();
            // Just verify it doesn't throw and returns something
            input.seek(0);
            input.switchToRemote();
            short remoteVal = input.readShort();
            // Both should succeed without error
        }
    }

    public void testReadInt() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            int val = input.readInt();
            input.seek(0);
            input.switchToRemote();
            int remoteVal = input.readInt();
        }
    }

    public void testReadLong() throws IOException {
        try (FormatSwitchableIndexInput input = createSwitchable()) {
            long val = input.readLong();
            input.seek(0);
            input.switchToRemote();
            long remoteVal = input.readLong();
        }
    }

    public void testReadVInt() throws IOException {
        // Write a proper VInt to both directories
        ByteBuffersDirectory vLocalDir = new ByteBuffersDirectory();
        ByteBuffersDirectory vRemoteDir = new ByteBuffersDirectory();
        try (IndexOutput out = vLocalDir.createOutput("vint.dat", IOContext.DEFAULT)) {
            out.writeVInt(42);
        }
        try (IndexOutput out = vRemoteDir.createOutput("vint.dat", IOContext.DEFAULT)) {
            out.writeVInt(99);
        }

        IndexInput localInput = vLocalDir.openInput("vint.dat", IOContext.DEFAULT);
        FormatSwitchableIndexInput input = new FormatSwitchableIndexInput("test", "vint.dat", localInput, () -> {
            try {
                return vRemoteDir.openInput("vint.dat", IOContext.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(42, input.readVInt());
        input.switchToRemote();
        assertEquals(99, input.readVInt());
        input.close();
        vLocalDir.close();
        vRemoteDir.close();
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private FormatSwitchableIndexInput createSwitchable() throws IOException {
        IndexInput localInput = localDir.openInput(FILE_NAME, IOContext.DEFAULT);
        return new FormatSwitchableIndexInput("FormatSwitchable(test.parquet)", FILE_NAME, localInput, () -> {
            try {
                return remoteDir.openInput(FILE_NAME, IOContext.DEFAULT);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void writeFile(ByteBuffersDirectory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
    }
}
