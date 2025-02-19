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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.plugin.wlm.RuleTestUtils.PATTERN_ONE;
import static org.opensearch.plugin.wlm.RuleTestUtils.SEARCH_AFTER;
import static org.opensearch.plugin.wlm.RuleTestUtils._ID_ONE;
import static org.opensearch.plugin.wlm.rule.QueryGroupAttribute.INDEX_PATTERN;

public class GetRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest
     */
    public void testSerialization() throws IOException {
        GetRuleRequest request = new GetRuleRequest(_ID_ONE, Map.of(INDEX_PATTERN, Set.of(PATTERN_ONE)), null);
        assertEquals(_ID_ONE, request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest when name is null
     */
    public void testSerializationWithNull() throws IOException {
        GetRuleRequest request = new GetRuleRequest((String) null, new HashMap<>(), SEARCH_AFTER);
        assertNull(request.get_id());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }
}
