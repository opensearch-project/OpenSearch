/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;

public class DeleteRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteRuleRequest
     */
    public void testSerialization() throws IOException {
        DeleteRuleRequest request = new DeleteRuleRequest(_ID_ONE);
        assertEquals(_ID_ONE, request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        DeleteRuleRequest otherRequest = new DeleteRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
    }

    /**
     * Test case to verify the serialization and deserialization of DeleteRuleRequest when ID is null
     */
    public void testSerializationWithNull() throws IOException {
        DeleteRuleRequest request = new DeleteRuleRequest((String) null);
        assertNull(request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        DeleteRuleRequest otherRequest = new DeleteRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
    }
}

