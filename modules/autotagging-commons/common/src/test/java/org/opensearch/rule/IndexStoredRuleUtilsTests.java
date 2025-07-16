/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.storage.IndexBasedRuleQueryMapper;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

public class IndexStoredRuleUtilsTests extends OpenSearchTestCase {
    RuleQueryMapper<QueryBuilder> sut;

    public void setUp() throws Exception {
        super.setUp();
        sut = new IndexBasedRuleQueryMapper();
    }

    public void testBuildGetRuleQuery_WithId() {
        QueryBuilder query = sut.from(
            new GetRuleRequest(RuleTestUtils._ID_ONE, new HashMap<>(), null, RuleTestUtils.MockRuleFeatureType.INSTANCE)
        );
        assertNotNull(query);
        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) query;
        assertEquals(1, queryBuilder.must().size());
        QueryBuilder idQuery = queryBuilder.must().get(0);
        assertTrue(idQuery.toString().contains(RuleTestUtils._ID_ONE));
    }

    public void testBuildGetRuleQuery_WithAttributes() {
        QueryBuilder queryBuilder = sut.from(
            new GetRuleRequest(null, RuleTestUtils.ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE)
        );
        assertNotNull(queryBuilder);
        BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder;
        assertEquals(1, query.must().size());
        assertTrue(query.toString().contains(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE.getName()));
        assertTrue(query.toString().contains(RuleTestUtils.ATTRIBUTE_VALUE_ONE));
    }
}
