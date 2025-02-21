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

public class SearchOnlyRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        SearchOnlyRequest request = new SearchOnlyRequest("test_index", true);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        SearchOnlyRequest deserializedRequest = new SearchOnlyRequest(in);

        assertEquals(request.getIndex(), deserializedRequest.getIndex());
        assertEquals(request.isScaleDown(), deserializedRequest.isScaleDown());
    }

    public void testValidation() {
        SearchOnlyRequest request = new SearchOnlyRequest(null, true);
        assertNotNull(request.validate());

        request = new SearchOnlyRequest("", true);
        assertNotNull(request.validate());

        request = new SearchOnlyRequest("  ", true);
        assertNotNull(request.validate());

        request = new SearchOnlyRequest("test_index", true);
        assertNull(request.validate());
    }

    public void testEquals() {
        SearchOnlyRequest request1 = new SearchOnlyRequest("test_index", true);
        SearchOnlyRequest request2 = new SearchOnlyRequest("test_index", true);
        SearchOnlyRequest request3 = new SearchOnlyRequest("other_index", true);
        SearchOnlyRequest request4 = new SearchOnlyRequest("test_index", false);

        assertEquals(request1, request2);
        assertNotEquals(request1, request3);
        assertNotEquals(request1, request4);
    }
}
