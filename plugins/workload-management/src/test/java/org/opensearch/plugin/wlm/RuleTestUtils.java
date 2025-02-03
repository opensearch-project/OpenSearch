/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.opensearch.wlm.Rule;
import org.opensearch.wlm.Rule.RuleAttribute;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertThrows;
import static org.opensearch.wlm.Rule._ID_STRING;
import static org.opensearch.wlm.Rule.builder;

public class RuleTestUtils {
    public static final String _ID_ONE = "AgfUO5Ja9yfvhdONlYi3TQ==";
    public static final String _ID_TWO = "G5iIq84j7eK1qIAAAAIH53=1";
    public static final String LABEL_ONE = "label_one";
    public static final String LABEL_TWO = "label_two";
    public static final String QUERY_GROUP = "query_group";
    public static final String TIMESTAMP_ONE = "2024-01-26T08:58:57.558Z";
    public static final String TIMESTAMP_TWO = "2023-01-26T08:58:57.558Z";
    public static final Rule ruleOne = builder()
        .feature(QUERY_GROUP)
        .label(LABEL_ONE)
        .attributeMap(Map.of(RuleAttribute.INDEX_PATTERN, Set.of("pattern_1")))
        .updatedAt(TIMESTAMP_ONE)
        .build();

    public static final Rule ruleTwo = builder()
        .feature(QUERY_GROUP)
        .label(LABEL_TWO)
        .attributeMap(Map.of(RuleAttribute.INDEX_PATTERN, Set.of("pattern_2", "pattern_3")))
        .updatedAt(TIMESTAMP_TWO)
        .build();

    public static Map<String, Rule> ruleMap() {
        return Map.of(
            _ID_ONE, ruleOne,
            _ID_TWO, ruleTwo
        );
    }

    public static void assertEqualRules(
        Map<String, Rule> mapOne,
        Map<String, Rule> mapTwo,
        boolean ruleUpdated
    ) {
        assertEquals(mapOne.size(), mapTwo.size());
        for (Map.Entry<String, Rule> entry: mapOne.entrySet()) {
            String id = entry.getKey();
            assertTrue(mapTwo.containsKey(id));
            Rule one = mapOne.get(id);
            Rule two = mapTwo.get(id);
            assertEqualRule(one, two, ruleUpdated);
        }
    }

    public static void assertEqualRule(Rule one,Rule two,boolean ruleUpdated) {
        if (ruleUpdated) {
            assertEquals(one.getFeature(), two.getFeature());
            assertEquals(one.getLabel(), two.getLabel());
            assertEquals(one.getAttributeMap(), two.getAttributeMap());
        } else {
            assertEquals(one, two);
        }
    }
}
