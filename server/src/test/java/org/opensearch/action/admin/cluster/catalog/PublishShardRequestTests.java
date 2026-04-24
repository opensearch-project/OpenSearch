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
        PublishShardRequest original = new PublishShardRequest("my-index", "my-catalog-repo");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        PublishShardRequest deserialized = new PublishShardRequest(in);

        assertArrayEquals(original.indices(), deserialized.indices());
        assertEquals(original.getCatalogRepoName(), deserialized.getCatalogRepoName());
    }

    public void testToString() {
        PublishShardRequest request = new PublishShardRequest("logs-2024", "iceberg-catalog");
        String str = request.toString();
        assertTrue(str.contains("logs-2024"));
        assertTrue(str.contains("iceberg-catalog"));
    }
}
