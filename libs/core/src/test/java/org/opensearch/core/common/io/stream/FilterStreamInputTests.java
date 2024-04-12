/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;

/** test the FilterStreamInput using the same BaseStreamTests */
public class FilterStreamInputTests extends BaseStreamTests {
    @Override
    protected StreamInput getStreamInput(BytesReference bytesReference) throws IOException {
        BytesRef br = bytesReference.toBytesRef();
        return new FilterStreamInput(StreamInput.wrap(br.bytes, br.offset, br.length)) {
        };
    }

    public void testMarkAndReset() throws IOException {
        FilterStreamInputTests filterStreamInputTests = new FilterStreamInputTests();

        ByteBuffer buffer = ByteBuffer.wrap(new byte[20]);
        for (int i = 0; i < buffer.limit(); i++) {
            buffer.put((byte) i);
        }
        buffer.rewind();
        BytesReference bytesReference = BytesReference.fromByteBuffer(buffer);
        StreamInput streamInput = filterStreamInputTests.getStreamInput(bytesReference);
        streamInput.read();
        assertThat(streamInput.markSupported(), is(true));
        streamInput.mark(-1);
        int int1 = streamInput.read();
        int int2 = streamInput.read();
        streamInput.reset();
        assertEquals(int1, streamInput.read());
        assertEquals(int2, streamInput.read());
    }
}
