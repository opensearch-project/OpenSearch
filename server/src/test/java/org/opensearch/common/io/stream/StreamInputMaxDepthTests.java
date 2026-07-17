/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io.stream;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class StreamInputMaxDepthTests extends OpenSearchTestCase {

    public void testDeeplyNestedArrayListThrows() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        for (int i = 0; i < 1200; i++) {
            out.writeByte((byte) 7);  // ArrayList type tag
            out.writeVInt(1);         // size = 1
        }
        out.writeByte((byte) 0);      // String type
        out.writeString("x");

        StreamInput in = out.bytes().streamInput();
        IOException ex = expectThrows(IOException.class, in::readGenericValue);
        assertTrue(ex.getMessage(), ex.getMessage().contains("Maximum nesting depth"));
    }

    public void testDeeplyNestedHashMapThrows() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        for (int i = 0; i < 1200; i++) {
            out.writeByte((byte) 10); // HashMap type tag
            out.writeVInt(1);         // size = 1
            out.writeString("k");     // key
        }
        out.writeByte((byte) 0);      // String type
        out.writeString("v");

        StreamInput in = out.bytes().streamInput();
        IOException ex = expectThrows(IOException.class, in::readGenericValue);
        assertTrue(ex.getMessage(), ex.getMessage().contains("Maximum nesting depth"));
    }

    public void testModerateNestingSucceeds() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        for (int i = 0; i < 50; i++) {
            out.writeByte((byte) 7);  // ArrayList
            out.writeVInt(1);
        }
        out.writeByte((byte) 0);      // String
        out.writeString("leaf");

        StreamInput in = out.bytes().streamInput();
        Object result = in.readGenericValue();
        assertNotNull(result);
    }
}
