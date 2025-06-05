/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rule.GetRuleRequest;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.SEARCH_AFTER;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;

public class GetRuleRequestTests extends OpenSearchTestCase {
    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest
     */
    public void testSerialization() throws IOException {
        GetRuleRequest request = new GetRuleRequest(_ID_ONE, ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertEquals(_ID_ONE, request.getId());
        assertNull(request.validate());
        assertNull(request.getSearchAfter());
        assertEquals(RuleTestUtils.MockRuleFeatureType.INSTANCE, request.getFeatureType());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    /**
     * Test case to verify the serialization and deserialization of GetRuleRequest when name is null
     */
    public void testSerializationWithNull() throws IOException {
        GetRuleRequest request = new GetRuleRequest(
            (String) null,
            new HashMap<>(),
            SEARCH_AFTER,
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );
        assertNull(request.getId());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetRuleRequest otherRequest = new GetRuleRequest(streamInput);
        assertEquals(request.getId(), otherRequest.getId());
        assertEquals(request.getAttributeFilters(), otherRequest.getAttributeFilters());
    }

    public void testValidate() {
        GetRuleRequest request = new GetRuleRequest("", ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertThrows(IllegalArgumentException.class, request::validate);
        request = new GetRuleRequest(_ID_ONE, ATTRIBUTE_MAP, "", RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertThrows(IllegalArgumentException.class, request::validate);
    }
}
