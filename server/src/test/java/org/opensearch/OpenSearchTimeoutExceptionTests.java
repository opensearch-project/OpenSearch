/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

public class OpenSearchTimeoutExceptionTests extends OpenSearchTestCase {

    public void testStatusIsGatewayTimeout() {
        OpenSearchTimeoutException e = new OpenSearchTimeoutException("request timed out");
        assertEquals(RestStatus.GATEWAY_TIMEOUT, e.status());
    }

    public void testStatusIsGatewayTimeoutWithCause() {
        OpenSearchTimeoutException e = new OpenSearchTimeoutException(new RuntimeException("cause"));
        assertEquals(RestStatus.GATEWAY_TIMEOUT, e.status());
    }
}
