/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.get;

import org.opensearch.Version;
import org.opensearch.action.support.master.info.ClusterInfoRequest;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;

public class GetIndexRequestTests extends OpenSearchTestCase {
    public void testGetIndexRequestExtendsClusterInfoRequestOfDeprecatedClassPath() {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices("test");
        assertThat(getIndexRequest instanceof ClusterInfoRequest, is(true));
    }

    public void testGetIndexRequestWriteableWithLatestNode() throws IOException {
        // Write to 2.17 stream
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            GetIndexRequest getIndexRequest = new GetIndexRequest().indices("test").addFeatures(GetIndexRequest.Feature.values());
            output.setVersion(Version.V_2_17_0);
            getIndexRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                GetIndexRequest getIndexRequestRead = new GetIndexRequest(in);
                assertArrayEquals(getIndexRequestRead.features(), getIndexRequest.features());
            }
        }
    }

    public void testGetIndexRequestWriteableWithOldNode() throws IOException {
        // Write to 2.1 stream
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            GetIndexRequest getIndexRequest = new GetIndexRequest().indices("test").addFeatures(GetIndexRequest.Feature.values());
            output.setVersion(Version.V_2_16_0);
            getIndexRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                GetIndexRequest getIndexRequestRead = new GetIndexRequest(in);
                assertTrue(Arrays.stream(getIndexRequestRead.features()).noneMatch(f -> f == GetIndexRequest.Feature.CONTEXT));
                assertEquals(3, getIndexRequestRead.features().length);
            }
        }
    }
}
