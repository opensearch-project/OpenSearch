/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PublishShardRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        PublishShardRequest original = new PublishShardRequest("publish-42", "my-index");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PublishShardRequest deserialized = new PublishShardRequest(in);

        assertArrayEquals(original.indices(), deserialized.indices());
        assertEquals(original.getPublishId(), deserialized.getPublishId());
    }

    public void testToString() {
        PublishShardRequest request = new PublishShardRequest("publish-xyz", "logs-2024");
        String str = request.toString();
        assertTrue("toString mentions index", str.contains("logs-2024"));
        assertTrue("toString mentions publishId", str.contains("publish-xyz"));
    }

    public void testNullPublishIdRejected() {
        expectThrows(NullPointerException.class, () -> new PublishShardRequest(null, "my-index"));
    }
}
