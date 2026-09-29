/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.action;

import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.rule.utils.RuleTestUtils.assertEqualRule;
import static org.opensearch.rule.utils.RuleTestUtils.ruleOne;

public class CreateRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateRuleRequest.
     */
    public void testSerialization() throws IOException {
        CreateRuleRequest request = new CreateRuleRequest(ruleOne);
        CreateRuleRequest otherRequest = copyWriteable(
            request,
            RuleTestUtils.namedWriteableRegistry(RuleTestUtils.MockRuleFeatureType.INSTANCE),
            CreateRuleRequest::new
        );
        assertEqualRule(ruleOne, otherRequest.getRule(), false);
    }
}
