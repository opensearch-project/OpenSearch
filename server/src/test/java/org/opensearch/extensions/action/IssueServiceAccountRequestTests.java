/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class IssueServiceAccountRequestTests extends OpenSearchTestCase {

    public void testIssueServiceAccountRequest() throws Exception {
        String serviceAccountToken = "testToken";
        IssueServiceAccountRequest request = new IssueServiceAccountRequest(serviceAccountToken);

        assertEquals(serviceAccountToken, request.getServiceAccountToken());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new IssueServiceAccountRequest(in);

        assertEquals(serviceAccountToken, request.getServiceAccountToken());
    }
}
