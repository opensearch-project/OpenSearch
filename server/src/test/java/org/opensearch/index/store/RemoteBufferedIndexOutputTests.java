/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.junit.After;
import org.junit.Before;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;

public class RemoteBufferedIndexOutputTests extends OpenSearchTestCase {

    private static final String FILENAME = "segment_1";

    private BlobContainer blobContainer;

    private BytesStreamOutput out;

    private OutputStreamIndexOutput indexOutputBuffer;

    private RemoteBufferedIndexOutput remoteBufferedIndexOutput;

    @Before
    public void setup() {
        blobContainer = mock(BlobContainer.class);
        out = new BytesStreamOutput();
        indexOutputBuffer = new OutputStreamIndexOutput(FILENAME, FILENAME, out, RemoteBufferedIndexOutput.BUFFER_SIZE);
        remoteBufferedIndexOutput = new RemoteBufferedIndexOutput(FILENAME, blobContainer, out, indexOutputBuffer);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        try (final BytesStreamOutput outStream = out) {
            indexOutputBuffer.close();
        } catch (IOException e) {
            // do nothing
        }
    }

    public void testCopyBytes() throws IOException {
        String testData = "testData";
        IndexInput indexInput = new ByteArrayIndexInput("blobName", testData.getBytes(StandardCharsets.UTF_8));
        remoteBufferedIndexOutput.copyBytes(indexInput, indexInput.length());
        indexOutputBuffer.getChecksum(); // calling getChecksum() to flush the buffer.
        assertEquals(out.bytes().utf8ToString(), testData);
        out.reset();
    }

    public void testCopyBytesException() throws IOException {
        OutputStreamIndexOutput indexOutputBufferMock = mock(OutputStreamIndexOutput.class);
        IndexInput indexInput = mock(IndexInput.class);
        RemoteBufferedIndexOutput bufferedIndexOutputUsingMock = new RemoteBufferedIndexOutput(
            FILENAME,
            blobContainer,
            out,
            indexOutputBufferMock
        );
        doThrow(new IOException("Test Induced Failure")).when(indexOutputBufferMock).copyBytes(eq(indexInput), eq(100L));

        assertThrows(IOException.class, () -> bufferedIndexOutputUsingMock.copyBytes(indexInput, 100));
    }

    public void testWriteBytes() throws IOException {
        byte[] b = new byte[] { Byte.MAX_VALUE };
        remoteBufferedIndexOutput.writeBytes(b, 0, b.length);
        indexOutputBuffer.getChecksum(); // calling getChecksum() to flush the buffer.
        assertArrayEquals(b, BytesReference.toBytes(out.bytes()));
        out.reset();
    }

    public void testClose() throws IOException {
        BytesReference mockBytesReference = mock(BytesReference.class);
        BytesStreamOutput outStream = mock(BytesStreamOutput.class);
        when(outStream.bytes()).thenReturn(mockBytesReference);
        when(mockBytesReference.streamInput()).thenReturn(mock(StreamInput.class));

        OutputStreamIndexOutput indexOutputBufferMock = mock(OutputStreamIndexOutput.class);
        RemoteBufferedIndexOutput bufferedIndexOutputUsingMock = new RemoteBufferedIndexOutput(
            FILENAME,
            blobContainer,
            outStream,
            indexOutputBufferMock
        );

        bufferedIndexOutputUsingMock.close();
        verify(blobContainer).writeBlob(eq(FILENAME), any(StreamInput.class), anyLong(), eq(false));
        verify(indexOutputBufferMock).close();
        verify(outStream).close();
    }

    public void testCloseException() throws IOException {
        BytesReference mockBytesReference = mock(BytesReference.class);
        BytesStreamOutput outStream = mock(BytesStreamOutput.class);
        when(outStream.bytes()).thenReturn(mockBytesReference);
        StreamInput streamInputMock = mock(StreamInput.class);
        when(mockBytesReference.streamInput()).thenReturn(streamInputMock);
        doThrow(new IOException("Test Induced Failure")).when(streamInputMock).close();

        OutputStreamIndexOutput indexOutputBufferMock = mock(OutputStreamIndexOutput.class);
        RemoteBufferedIndexOutput bufferedIndexOutputUsingMock = new RemoteBufferedIndexOutput(
            FILENAME,
            blobContainer,
            outStream,
            indexOutputBufferMock
        );
        try {
            bufferedIndexOutputUsingMock.close();
        } catch (Exception e) {
            // do nothing
        }
        verify(blobContainer).writeBlob(eq(FILENAME), any(StreamInput.class), anyLong(), eq(false));
        verify(indexOutputBufferMock).close();
        verify(outStream).close();

    }

    public void testCloseException2() throws IOException {
        BytesReference mockBytesReference = mock(BytesReference.class);
        BytesStreamOutput outStream = mock(BytesStreamOutput.class);
        when(outStream.bytes()).thenReturn(mockBytesReference);
        StreamInput streamInputMock = mock(StreamInput.class);
        when(mockBytesReference.streamInput()).thenReturn(streamInputMock);

        OutputStreamIndexOutput indexOutputBufferMock = mock(OutputStreamIndexOutput.class);
        doThrow(new IOException("Test Induced Failure")).when(indexOutputBufferMock).close();
        RemoteBufferedIndexOutput bufferedIndexOutputUsingMock = new RemoteBufferedIndexOutput(
            FILENAME,
            blobContainer,
            outStream,
            indexOutputBufferMock
        );
        try {
            bufferedIndexOutputUsingMock.close();
        } catch (Exception e) {
            // do nothing
        }
        verify(indexOutputBufferMock).close();
        verify(outStream).close();
    }
}
