/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DecommissionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String attributeName = "zone";
        String attributeValue = "zone-1";
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));
        final DecommissionRequest originalRequest = new DecommissionRequest(decommissionAttribute, timeout);

        final DecommissionRequest deserialized = copyWriteable(originalRequest, writableRegistry(), DecommissionRequest::new);

        assertEquals(deserialized.getDecommissionAttribute(), originalRequest.getDecommissionAttribute());
        assertEquals(deserialized.getTimeout(), originalRequest.getTimeout());
    }

    public void testValidation() {
        {
            String attributeName = null;
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
            TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute, timeout);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute name is missing"));
        }
        {
            String attributeName = "zone";
            String attributeValue = "";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
            TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute, timeout);
            ActionRequestValidationException e = request.validate();
            assertNotNull(e);
            assertTrue(e.getMessage().contains("attribute value is missing"));
        }
        {
            String attributeName = "zone";
            String attributeValue = "test";
            DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
            TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));

            final DecommissionRequest request = new DecommissionRequest(decommissionAttribute, timeout);
            ActionRequestValidationException e = request.validate();
            assertNull(e);
        }
    }
}
