/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness;

import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteDecommissionRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final DeleteDecommissionRequest originalRequest = new DeleteDecommissionRequest();

        final DeleteDecommissionRequest deserialized = copyWriteable(originalRequest, writableRegistry(), DeleteDecommissionRequest::new);
        assertEquals(deserialized, originalRequest);
    }
}
