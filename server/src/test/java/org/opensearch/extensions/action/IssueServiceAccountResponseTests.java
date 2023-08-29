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

public class IssueServiceAccountResponseTests extends OpenSearchTestCase {

    public void testIssueServiceAccountResponse() throws Exception {
        String extensionName = "testExtension";
        String serviceAccountToken = "testToken";
        IssueServiceAccountResponse response = new IssueServiceAccountResponse(extensionName, serviceAccountToken);

        assertEquals(extensionName, response.getName());
        assertEquals(serviceAccountToken, response.getServiceAccountString());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        response = new IssueServiceAccountResponse(in);
        assertEquals(extensionName, response.getName());
        assertEquals(serviceAccountToken, response.getServiceAccountString());
    }
}
