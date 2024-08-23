/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.wlm.QueryGroupTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteQueryGroupRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteQueryGroupRequest.
     */
    public void testSerialization() throws IOException {
        DeleteQueryGroupRequest request = new DeleteQueryGroupRequest(QueryGroupTestUtils.NAME_ONE);
        assertEquals(QueryGroupTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        DeleteQueryGroupRequest otherRequest = new DeleteQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to validate a DeleteQueryGroupRequest.
     */
    public void testSerializationWithNull() throws IOException {
        DeleteQueryGroupRequest request = new DeleteQueryGroupRequest((String) null);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertFalse(actionRequestValidationException.getMessage().isEmpty());
    }
}
