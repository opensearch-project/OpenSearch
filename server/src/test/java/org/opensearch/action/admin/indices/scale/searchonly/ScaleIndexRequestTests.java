/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class ScaleIndexRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        ScaleIndexRequest request = new ScaleIndexRequest("test_index", true);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ScaleIndexRequest deserializedRequest = new ScaleIndexRequest(in);

        assertEquals(request.getIndex(), deserializedRequest.getIndex());
        assertEquals(request.isScaleDown(), deserializedRequest.isScaleDown());
    }

    public void testValidation() {
        ScaleIndexRequest request = new ScaleIndexRequest(null, true);
        assertNotNull(request.validate());

        request = new ScaleIndexRequest("", true);
        assertNotNull(request.validate());

        request = new ScaleIndexRequest("  ", true);
        assertNotNull(request.validate());

        request = new ScaleIndexRequest("test_index", true);
        assertNull(request.validate());
    }

    public void testEquals() {
        ScaleIndexRequest request1 = new ScaleIndexRequest("test_index", true);
        ScaleIndexRequest request2 = new ScaleIndexRequest("test_index", true);
        ScaleIndexRequest request3 = new ScaleIndexRequest("other_index", true);
        ScaleIndexRequest request4 = new ScaleIndexRequest("test_index", false);

        assertEquals(request1, request2);
        assertNotEquals(request1, request3);
        assertNotEquals(request1, request4);
    }
}
