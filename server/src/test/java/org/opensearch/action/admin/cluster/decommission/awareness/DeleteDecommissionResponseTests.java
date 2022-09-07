/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness;

import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class DeleteDecommissionResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final DeleteDecommissionResponse originalResponse = new DeleteDecommissionResponse(true);

        final DeleteDecommissionResponse deserialized = copyWriteable(
            originalResponse,
            writableRegistry(),
            DeleteDecommissionResponse::new
        );
        assertEquals(deserialized, originalResponse);

    }
}
