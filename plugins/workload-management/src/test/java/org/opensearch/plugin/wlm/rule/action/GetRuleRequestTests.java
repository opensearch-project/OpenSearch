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

public class GetRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest
     */
    public void testSerialization() throws IOException {
        GetRuleRequest request = new GetRuleRequest(_ID_ONE);
        assertEquals(_ID_ONE, request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest when name is null
     */
    public void testSerializationWithNull() throws IOException {
        GetRuleRequest request = new GetRuleRequest((String) null);
        assertNull(request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
    }
}
