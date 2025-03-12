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
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.DESCRIPTION_ONE;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;

public class UpdateRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateRuleRequest.
     */
    public void testSerialization() throws IOException {
        UpdateRuleRequest request = new UpdateRuleRequest(
            _ID_ONE,
            DESCRIPTION_ONE,
            ATTRIBUTE_MAP,
            FEATURE_VALUE_ONE,
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateRuleRequest otherRequest = new UpdateRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
        assertEquals(request.getFeatureValue(), otherRequest.getFeatureValue());
        assertEquals(request.getAttributeMap(), otherRequest.getAttributeMap());
        assertEquals(request.getDescription(), otherRequest.getDescription());
    }

    /**
     * Test case to verify the serialization and deserialization of UpdateRuleRequest when there's null values.
     */
    public void testSerializationWithNull() throws IOException {
        UpdateRuleRequest request = new UpdateRuleRequest(_ID_ONE, null, ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateRuleRequest otherRequest = new UpdateRuleRequest(streamInput);
        assertEquals(request.get_id(), otherRequest.get_id());
        assertEquals(request.getFeatureValue(), otherRequest.getFeatureValue());
        assertEquals(request.getAttributeMap(), otherRequest.getAttributeMap());
        assertEquals(request.getDescription(), otherRequest.getDescription());
    }
}
