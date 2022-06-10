/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.*;

public class RemoteIndexInputTests extends OpenSearchTestCase {

    private static final String FILENAME = "segment_1";
    private static final long FILESIZE = 200;

    private InputStream inputStream;
    private RemoteIndexInput remoteIndexInput;

    @Before
    public void setup() {
        inputStream = mock(InputStream.class);
        remoteIndexInput = new RemoteIndexInput(FILENAME, inputStream, FILESIZE);
    }

    public void testReadByte() throws IOException {
        InputStream inputStream = spy(InputStream.class);
        remoteIndexInput = new RemoteIndexInput(FILENAME, inputStream, FILESIZE);

        when(inputStream.read()).thenReturn(10);

        assertEquals(10, remoteIndexInput.readByte());

        verify(inputStream).read(any());
    }

    public void testReadByteIOException() throws IOException {
        when(inputStream.read(any())).thenThrow(new IOException("Error reading"));

        assertThrows(IOException.class, () -> remoteIndexInput.readByte());
    }

    public void testReadBytes() throws IOException {
        byte[] buffer = new byte[10];
        remoteIndexInput.readBytes(buffer, 10, 20);

        verify(inputStream).read(buffer, 10, 20);
    }

    public void testReadBytesIOException() throws IOException {
        byte[] buffer = new byte[10];
        when(inputStream.read(buffer, 10, 20)).thenThrow(new IOException("Error reading"));

        assertThrows(IOException.class, () -> remoteIndexInput.readBytes(buffer, 10, 20));
    }

    public void testClose() throws IOException {
        remoteIndexInput.close();

        verify(inputStream).close();
    }

    public void testCloseIOException() throws IOException {
        doThrow(new IOException("Error closing")).when(inputStream).close();

        assertThrows(IOException.class, () -> remoteIndexInput.close());
    }

    public void testLength() {
        assertEquals(FILESIZE, remoteIndexInput.length());
    }

    public void testSeek() throws IOException {
        remoteIndexInput.seek(10);

        verify(inputStream).skip(10);
    }

    public void testSeekIOException() throws IOException {
        when(inputStream.skip(10)).thenThrow(new IOException("Error reading"));

        assertThrows(IOException.class, () -> remoteIndexInput.seek(10));
    }

    public void testGetFilePointer() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexInput.getFilePointer());
    }

    public void testSlice() {
        assertThrows(UnsupportedOperationException.class, () -> remoteIndexInput.slice("Slice middle", 50, 100));
    }
}
