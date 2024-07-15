/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class CreateQueryGroupRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        CreateQueryGroupRequest request = new CreateQueryGroupRequest(QueryGroupTestUtils.queryGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateQueryGroupRequest otherRequest = new CreateQueryGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
        assertEquals(request.getResourceLimits().size(), otherRequest.getResourceLimits().size());
        assertEquals(request.getResiliencyMode(), otherRequest.getResiliencyMode());
        QueryGroupTestUtils.compareResourceLimits(request.getResourceLimits(), otherRequest.getResourceLimits());
        assertEquals(request.getUpdatedAtInMillis(), otherRequest.getUpdatedAtInMillis());
    }
}
