/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.decommission.awareness.put;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class PutDecommissionResponseTests extends OpenSearchTestCase {
    public void testSerialization() throws IOException {
        final PutDecommissionResponse originalRequest = new PutDecommissionResponse(true);
        copyWriteable(originalRequest, writableRegistry(), PutDecommissionResponse::new);
        // there are no fields so we're just checking that this doesn't throw anything
    }
}
