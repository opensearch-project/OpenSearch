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
import org.opensearch.plugin.wlm.rule.QueryGroupFeatureType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.opensearch.plugin.wlm.rule.QueryGroupAttribute.INDEX_PATTERN;
import static org.opensearch.plugin.wlm.rule.WlmRuleTestUtils.PATTERN_ONE;
import static org.opensearch.plugin.wlm.rule.WlmRuleTestUtils.SEARCH_AFTER;
import static org.opensearch.plugin.wlm.rule.WlmRuleTestUtils._ID_ONE;

public class GetWlmRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest
     */
    public void testSerialization() throws IOException {
        GetWlmRuleRequest request = new GetWlmRuleRequest(
            _ID_ONE,
            Map.of(INDEX_PATTERN, Set.of(PATTERN_ONE)),
            null,
            QueryGroupFeatureType.INSTANCE
        );
        assertEquals(_ID_ONE, request.getId());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWlmRuleRequest otherRequest = new GetWlmRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest when name is null
     */
    public void testSerializationWithNull() throws IOException {
        GetWlmRuleRequest request = new GetWlmRuleRequest((String) null, new HashMap<>(), SEARCH_AFTER, QueryGroupFeatureType.INSTANCE);
        assertNull(request.getId());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWlmRuleRequest otherRequest = new GetWlmRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }
}
