/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.action;

import org.opensearch.cluster.metadata.Rule;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.plugin.wlm.RuleTestUtils.ruleOne;
import static org.opensearch.plugin.wlm.RuleTestUtils.assertEqualRules;

public class CreateRuleRequestTests extends OpenSearchTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateRuleRequest.
     */
    public void testSerialization() throws IOException {
        CreateRuleRequest request = new CreateRuleRequest(ruleOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateRuleRequest otherRequest = new CreateRuleRequest(streamInput);
        List<Rule> list1 = new ArrayList<>();
        List<Rule> list2 = new ArrayList<>();
        list1.add(ruleOne);
        list2.add(otherRequest.getRule());
        assertEqualRules(list1, list2, false);
    }
}
