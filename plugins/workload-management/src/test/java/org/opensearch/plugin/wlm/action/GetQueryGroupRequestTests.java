/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.QueryGroupTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetQueryGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetQueryGroupRequest.
     */
    public void testSerialization() throws IOException {
        GetQueryGroupRequest request = new GetQueryGroupRequest(QueryGroupTestUtils.NAME_ONE);
        assertEquals(QueryGroupTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetQueryGroupRequest otherRequest = new GetQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to verify the serialization and deserialization of GetQueryGroupRequest when name is null.
     */
    public void testSerializationWithNull() throws IOException {
        GetQueryGroupRequest request = new GetQueryGroupRequest((String) null);
        assertNull(request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetQueryGroupRequest otherRequest = new GetQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case the validation function of GetQueryGroupRequest
     */
    public void testValidation() {
        GetQueryGroupRequest request = new GetQueryGroupRequest("a".repeat(51));
        assertThrows(IllegalArgumentException.class, request::validate);
    }
}
