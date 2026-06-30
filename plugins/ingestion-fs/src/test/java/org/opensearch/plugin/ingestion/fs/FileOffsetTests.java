/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;

public class FileOffsetTests extends OpenSearchTestCase {

    public void testFileOffset() {
        FileOffset offset = new FileOffset(42L);
        byte[] serialized = offset.serialize();
        long deserialized = ByteBuffer.wrap(serialized).getLong();
        assertEquals(offset.getLine(), deserialized);
    }
}
