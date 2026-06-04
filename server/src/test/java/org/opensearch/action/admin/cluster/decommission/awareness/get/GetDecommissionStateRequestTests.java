/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.get;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetDecommissionStateRequestTests extends OpenSearchTestCase {
    public void testSerialization() throws IOException {
        String attributeName = "zone";
        final GetDecommissionStateRequest originalRequest = new GetDecommissionStateRequest(attributeName);
        final GetDecommissionStateRequest deserialized = copyWriteable(
            originalRequest,
            writableRegistry(),
            GetDecommissionStateRequest::new
        );
        assertEquals(deserialized.attributeName(), originalRequest.attributeName());
    }

    public void testValidation() {
        {
            String attributeName = null;
            final GetDecommissionStateRequest request = new GetDecommissionStateRequest(attributeName);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute name is missing"));
        }
        {
            String attributeName = "";
            final GetDecommissionStateRequest request = new GetDecommissionStateRequest(attributeName);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute name is missing"));
        }
        {
            String attributeName = "zone";
            final GetDecommissionStateRequest request = new GetDecommissionStateRequest(attributeName);
            ActionRequestValidationException e = request.validate();
            assertNull(e);
        }
    }
}
