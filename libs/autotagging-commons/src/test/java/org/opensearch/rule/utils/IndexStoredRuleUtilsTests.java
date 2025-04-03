/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.utils;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

import static org.opensearch.rule.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.RuleTestUtils._ID_ONE;

public class IndexStoredRuleUtilsTests extends OpenSearchTestCase {
    public void testBuildGetRuleQuery_WithId() {
        BoolQueryBuilder query = IndexStoredRuleUtils.buildGetRuleQuery(
            _ID_ONE,
            new HashMap<>(),
            RuleTestUtils.MockRuleFeatureType.INSTANCE
        );
        assertNotNull(query);
        assertEquals(1, query.must().size());
        QueryBuilder idQuery = query.must().get(0);
        assertTrue(idQuery.toString().contains(_ID_ONE));
    }

    public void testBuildGetRuleQuery_WithAttributes() {
        BoolQueryBuilder query = IndexStoredRuleUtils.buildGetRuleQuery(null, ATTRIBUTE_MAP, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertNotNull(query);
        assertTrue(query.must().size() == 1);
        assertTrue(query.toString().contains(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE.getName()));
        assertTrue(query.toString().contains(ATTRIBUTE_VALUE_ONE));
    }
}
