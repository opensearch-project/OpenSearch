/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.junit.Before;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class RemoteBufferedIndexOutputTests extends OpenSearchTestCase {

    private static final String FILENAME = "segment_1";

    private BlobContainer blobContainer;

    private BytesStreamOutput out;

    private OutputStreamIndexOutput indexOutputBuffer;

    private RemoteBufferedIndexOutput remoteBufferedIndexOutput;

    @Before
    public void setup() {
        blobContainer = mock(BlobContainer.class);
        out = mock(BytesStreamOutput.class);
        indexOutputBuffer = mock(OutputStreamIndexOutput.class);

        remoteBufferedIndexOutput = new RemoteBufferedIndexOutput(FILENAME, blobContainer, out, indexOutputBuffer);
    }

    public void testCopyBytes() throws IOException {
        IndexInput indexInput = mock(IndexInput.class);
        remoteBufferedIndexOutput.copyBytes(indexInput, 100);

        verify(indexOutputBuffer).copyBytes(eq(indexInput), eq(100L));
    }

    public void testCopyBytesException() throws IOException {
        IndexInput indexInput = mock(IndexInput.class);
        doThrow(new IOException("Test Induced Failure")).when(indexOutputBuffer).copyBytes(eq(indexInput), eq(100L));

        assertThrows(IOException.class, () -> remoteBufferedIndexOutput.copyBytes(indexInput, 100));
    }

    public void testClose() throws IOException {
        BytesReference mockBytesReference = mock(BytesReference.class);
        when(out.bytes()).thenReturn(mockBytesReference);
        when(mockBytesReference.streamInput()).thenReturn(mock(StreamInput.class));

        remoteBufferedIndexOutput.close();
        verify(blobContainer).writeBlob(eq(FILENAME), any(StreamInput.class), anyLong(), eq(false));
        verify(indexOutputBuffer).close();
        verify(out).close();
    }
}
