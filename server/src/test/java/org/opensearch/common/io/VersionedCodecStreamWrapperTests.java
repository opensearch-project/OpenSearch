/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.junit.Before;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link org.opensearch.common.io.VersionedCodecStreamWrapper}.
 */
public class VersionedCodecStreamWrapperTests extends OpenSearchTestCase {

    private static final String CODEC = "dummycodec";
    private static final int VERSION = 1;

    IndexIOStreamHandler<DummyObject> ioStreamHandler;
    VersionedCodecStreamWrapper<DummyObject> versionedCodecStreamWrapper;

    @Before
    public void setup() throws IOException {
        ioStreamHandler = mock(IndexIOStreamHandler.class);
        versionedCodecStreamWrapper = new VersionedCodecStreamWrapper(ioStreamHandler, VERSION, CODEC);
    }

    public void testReadStream() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        DummyObject readData = versionedCodecStreamWrapper.readStream(createHeaderFooterBytes(CODEC, VERSION, true, true));
        assertEquals(readData, expectedObject);
    }

    public void testReadWithOldVersionThrowsException() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        assertThrows(
            IndexFormatTooOldException.class,
            () -> versionedCodecStreamWrapper.readStream(createHeaderFooterBytes(CODEC, 0, true, true))
        );
    }

    public void testReadWithNewVersionThrowsException() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        assertThrows(
            IndexFormatTooNewException.class,
            () -> versionedCodecStreamWrapper.readStream(createHeaderFooterBytes(CODEC, 2, true, true))
        );
    }

    public void testReadWithUnexpectedCodecThrowsException() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        assertThrows(
            CorruptIndexException.class,
            () -> versionedCodecStreamWrapper.readStream(createHeaderFooterBytes("wrong codec", VERSION, true, true))
        );
    }

    public void testReadWithNoHeaderThrowsException() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        assertThrows(
            CorruptIndexException.class,
            () -> versionedCodecStreamWrapper.readStream(createHeaderFooterBytes("wrong codec", VERSION, false, true))
        );
    }

    public void testReadWithNoFooterThrowsException() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        when(ioStreamHandler.readContent(any())).thenReturn(expectedObject);
        assertThrows(
            CorruptIndexException.class,
            () -> versionedCodecStreamWrapper.readStream(createHeaderFooterBytes("wrong codec", VERSION, true, false))
        );
    }

    public void testWriteStream() throws IOException {
        DummyObject expectedObject = new DummyObject("test read");
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        doAnswer(invocation -> {
            IndexOutput io = invocation.getArgument(0);
            io.writeString("test write");
            return null;
        }).when(ioStreamHandler).writeContent(indexOutput, expectedObject);
        versionedCodecStreamWrapper.writeStream(indexOutput, expectedObject);
        indexOutput.close();
        IndexInput indexInput = new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()));
        BufferedChecksumIndexInput bii = new BufferedChecksumIndexInput(indexInput);

        CodecUtil.checkHeader(bii, CODEC, VERSION, VERSION);
        assertEquals(bii.readString(), "test write");
        CodecUtil.checkFooter(bii);
    }

    private ByteArrayIndexInput createHeaderFooterBytes(String codec, int version, boolean writeHeader, boolean writeFooter)
        throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput("dummy bytes", "dummy stream", output, 4096);
        if (writeHeader) {
            CodecUtil.writeHeader(indexOutput, codec, version);
        }
        if (writeFooter) {
            CodecUtil.writeFooter(indexOutput);
        }
        indexOutput.close();
        return new ByteArrayIndexInput("dummy bytes", BytesReference.toBytes(output.bytes()));
    }

    private static class DummyObject {
        private static String dummyString;

        public DummyObject(String dummy) {
            dummyString = dummy;
        }
    }
}
