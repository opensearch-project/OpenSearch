/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.open;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class OpenIndexRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        OpenIndexRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final OpenIndexRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new OpenIndexRequest(in);
            }
            assertEquals(request.timeout(), deserializedRequest.timeout());
            assertEquals(request.clusterManagerNodeTimeout(), deserializedRequest.clusterManagerNodeTimeout());
            assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            assertEquals(request.getParentTask(), deserializedRequest.getParentTask());
            assertEquals(request.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            assertArrayEquals(request.indices(), deserializedRequest.indices());
        }
    }

    public void testGetDescription() {
        String indexName = randomAlphaOfLengthBetween(3, 10);
        OpenIndexRequest request = new OpenIndexRequest(indexName);
        assertEquals("open indices [" + indexName + "]", request.getDescription());
    }

    private OpenIndexRequest randomRequest() {
        OpenIndexRequest request = new OpenIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
        }
        if (randomBoolean()) {
            request.timeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.clusterManagerNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.waitForActiveShards(
                randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.ALL)
            );
        }
        return request;
    }
}
