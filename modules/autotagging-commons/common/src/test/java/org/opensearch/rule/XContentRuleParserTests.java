/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.XContentRuleParser;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Locale;

import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;

public class XContentRuleParserTests extends OpenSearchTestCase {
    public static final String VALID_JSON = String.format(Locale.ROOT, """
        {
            "id": "%s",
            "description": "%s",
            "mock_feature_type": "feature value",
            "mock_attribute_one": ["attribute_value_one", "attribute_value_two"],
            "updated_at": "%s"
        }
        """, _ID_ONE, RuleTestUtils.DESCRIPTION_ONE, Instant.now().toString());

    private static final String INVALID_JSON = """
        {
            "name": "TestRule",
            "description": "A test rule for unit testing",
            "mock_attribute_three": ["attribute_value_one", "attribute_value_two"]
        }
        """;
    RuleEntityParser sut;

    public void setUp() throws Exception {
        super.setUp();
        sut = new XContentRuleParser(RuleTestUtils.MockRuleFeatureType.INSTANCE);
    }

    public void testParseRule_Success() throws IOException {
        Rule parsedRule = sut.parse(VALID_JSON);
        assertNotNull(parsedRule);
        assertEquals(RuleTestUtils.DESCRIPTION_ONE, parsedRule.getDescription());
        assertEquals(RuleTestUtils.MockRuleFeatureType.INSTANCE, parsedRule.getFeatureType());
    }

    public void testParseRule_InvalidJson() {
        Exception exception = assertThrows(RuntimeException.class, () -> sut.parse(INVALID_JSON));
        assertTrue(exception.getMessage().contains("mock_attribute_three is not a valid attribute within the mock_feature_type feature."));
    }
}
