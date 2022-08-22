/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PutDecommissionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        String attributeName = "zone";
        String attributeValue = "zone-1";
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));
        final PutDecommissionRequest originalRequest = new PutDecommissionRequest(
            decommissionAttribute,
            timeout
        );

        final PutDecommissionRequest deserialized = copyWriteable(
            originalRequest,
            writableRegistry(),
            PutDecommissionRequest::new
        );

        assertEquals(deserialized.getDecommissionAttribute(), originalRequest.getDecommissionAttribute());
        assertEquals(deserialized.getTimeout(), originalRequest.getTimeout());
    }
}
