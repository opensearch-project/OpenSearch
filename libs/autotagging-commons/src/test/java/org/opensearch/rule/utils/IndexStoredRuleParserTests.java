/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.opensearch.autotagging.Rule;
import org.opensearch.rule.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;

import static org.opensearch.rule.RuleTestUtils.DESCRIPTION_ONE;

public class IndexStoredRuleParserTests extends OpenSearchTestCase {
    public static final String VALID_JSON = String.format(Locale.ROOT, """
        {
            "description": "%s",
            "mock_feature_type": "feature value",
            "mock_attribute_one": ["attribute_value_one", "attribute_value_two"],
            "updated_at": "%s"
        }
        """, DESCRIPTION_ONE, Instant.now().toString());

    private static final String INVALID_JSON = """
        {
            "name": "TestRule",
            "description": "A test rule for unit testing",
            "mock_attribute_three": ["attribute_value_one", "attribute_value_two"]
        }
        """;

    public void testParseRule_Success() throws IOException {
        Rule parsedRule = IndexStoredRuleParser.parseRule(VALID_JSON, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertNotNull(parsedRule);
        assertEquals(DESCRIPTION_ONE, parsedRule.getDescription());
        assertEquals(RuleTestUtils.MockRuleFeatureType.INSTANCE, parsedRule.getFeatureType());
    }

    public void testParseRule_InvalidJson() {
        Exception exception = assertThrows(
            RuntimeException.class,
            () -> IndexStoredRuleParser.parseRule(INVALID_JSON, RuleTestUtils.MockRuleFeatureType.INSTANCE)
        );
        assertTrue(exception.getMessage().contains("mock_attribute_three is not a valid attribute within the mock_feature_type feature."));
    }

}
