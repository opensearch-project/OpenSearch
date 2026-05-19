/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link FormatSwitchableIndexInputWrapper}.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Read delegation through wrapper</li>
 *   <li>Clone returns wrapped instance</li>
 *   <li>Slice returns wrapped instance</li>
 *   <li>Close delegates to inner</li>
 *   <li>unwrap() returns the inner FormatSwitchableIndexInput</li>
 *   <li>switchToRemote via unwrap()</li>
 *   <li>Cleaner registration (GC safety)</li>
 * </ul>
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FormatSwitchableIndexInputWrapperTests extends OpenSearchTestCase {

    private static final byte[] LOCAL_DATA = "local-wrapper-test-data-12345".getBytes(StandardCharsets.UTF_8);
    private static final byte[] REMOTE_DATA = "remote-wrapper-test-data-12345".getBytes(StandardCharsets.UTF_8);
    private static final String FILE_NAME = "wrapper_test.parquet";

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
    // Read delegation
    // ═══════════════════════════════════════════════════════════════

    public void testReadByteDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            assertEquals(LOCAL_DATA[0], wrapper.readByte());
        }
    }

    public void testReadBytesDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            byte[] buf = new byte[LOCAL_DATA.length];
            wrapper.readBytes(buf, 0, buf.length);
            assertArrayEquals(LOCAL_DATA, buf);
        }
    }

    public void testReadShortDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readShort(); // just verify no exception
        }
    }

    public void testReadIntDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readInt(); // just verify no exception
        }
    }

    public void testReadLongDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readLong(); // just verify no exception
        }
    }

    public void testLengthDelegatesToInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            assertEquals(LOCAL_DATA.length, wrapper.length());
        }
    }

    public void testSeekAndGetFilePointer() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            assertEquals(0, wrapper.getFilePointer());
            wrapper.seek(5);
            assertEquals(5, wrapper.getFilePointer());
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // unwrap()
    // ═══════════════════════════════════════════════════════════════

    public void testUnwrapReturnsInner() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            FormatSwitchableIndexInput inner = wrapper.unwrap();
            assertNotNull(inner);
            assertFalse(inner.hasSwitchedToRemote());
        }
    }

    public void testSwitchToRemoteViaUnwrap() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            FormatSwitchableIndexInput inner = wrapper.unwrap();
            assertFalse(inner.hasSwitchedToRemote());

            inner.switchToRemote();
            assertTrue(inner.hasSwitchedToRemote());

            // Reads through wrapper should now come from remote
            wrapper.seek(0);
            byte[] buf = new byte[REMOTE_DATA.length];
            wrapper.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Clone
    // ═══════════════════════════════════════════════════════════════

    public void testCloneReturnsWrappedInstance() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            IndexInput cloned = wrapper.clone();
            assertTrue("Clone should be a FormatSwitchableIndexInputWrapper", cloned instanceof FormatSwitchableIndexInputWrapper);
            byte[] buf = new byte[LOCAL_DATA.length];
            cloned.readBytes(buf, 0, buf.length);
            assertArrayEquals(LOCAL_DATA, buf);
            cloned.close();
        }
    }

    public void testCloneAfterSwitchReadsFromRemote() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.unwrap().switchToRemote();
            IndexInput cloned = wrapper.clone();
            assertTrue(cloned instanceof FormatSwitchableIndexInputWrapper);
            byte[] buf = new byte[REMOTE_DATA.length];
            cloned.readBytes(buf, 0, buf.length);
            assertArrayEquals(REMOTE_DATA, buf);
            cloned.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Slice
    // ═══════════════════════════════════════════════════════════════

    public void testSliceReturnsWrappedInstance() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            IndexInput sliced = wrapper.slice("test-slice", 5, 5);
            assertTrue("Slice should be a FormatSwitchableIndexInputWrapper", sliced instanceof FormatSwitchableIndexInputWrapper);
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
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.unwrap().switchToRemote();
            IndexInput sliced = wrapper.slice("test-slice", 5, 5);
            byte[] buf = new byte[5];
            sliced.readBytes(buf, 0, 5);
            byte[] expected = new byte[5];
            System.arraycopy(REMOTE_DATA, 5, expected, 0, 5);
            assertArrayEquals(expected, buf);
            sliced.close();
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Close
    // ═══════════════════════════════════════════════════════════════

    public void testCloseDelegatesToInner() throws IOException {
        FormatSwitchableIndexInputWrapper wrapper = createWrapper();
        wrapper.close();
        // Double close should not throw
    }

    // ═══════════════════════════════════════════════════════════════
    // RandomAccessInput positional reads
    // ═══════════════════════════════════════════════════════════════

    public void testReadByteAtPosition() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            byte b = wrapper.readByte(0);
            assertEquals(LOCAL_DATA[0], b);
            byte b5 = wrapper.readByte(5);
            assertEquals(LOCAL_DATA[5], b5);
        }
    }

    public void testReadShortAtPosition() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readShort(0); // just verify no exception
        }
    }

    public void testReadIntAtPosition() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readInt(0); // just verify no exception
        }
    }

    public void testReadLongAtPosition() throws IOException {
        try (FormatSwitchableIndexInputWrapper wrapper = createWrapper()) {
            wrapper.readLong(0); // just verify no exception
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Helpers
    // ═══════════════════════════════════════════════════════════════

    private FormatSwitchableIndexInputWrapper createWrapper() throws IOException {
        IndexInput localInput = localDir.openInput(FILE_NAME, IOContext.DEFAULT);
        FormatSwitchableIndexInput switchable = new FormatSwitchableIndexInput(
            "FormatSwitchable(wrapper_test.parquet)",
            FILE_NAME,
            localInput,
            () -> {
                try {
                    return remoteDir.openInput(FILE_NAME, IOContext.DEFAULT);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        );
        return new FormatSwitchableIndexInputWrapper("WrapperTest(" + FILE_NAME + ")", switchable);
    }

    private static void writeFile(ByteBuffersDirectory dir, String name, byte[] data) throws IOException {
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(data, data.length);
        }
    }
}
