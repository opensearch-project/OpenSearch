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
import org.opensearch.rule.storage.IndexBasedRuleQueryBuilder;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;

import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_MAP;
import static org.opensearch.rule.utils.RuleTestUtils.ATTRIBUTE_VALUE_ONE;
import static org.opensearch.rule.utils.RuleTestUtils._ID_ONE;

public class IndexStoredRuleUtilsTests extends OpenSearchTestCase {
    RuleQueryBuilder<QueryBuilder> sut;

    public void setUp() throws Exception {
        super.setUp();
        sut = new IndexBasedRuleQueryBuilder();
    }

    public void testBuildGetRuleQuery_WithId() {
        QueryBuilder query = sut.buildQuery(new GetRuleRequest(_ID_ONE, new HashMap<>(), null, RuleTestUtils.MockRuleFeatureType.INSTANCE));
        assertNotNull(query);
        BoolQueryBuilder queryBuilder = (BoolQueryBuilder) query;
        assertEquals(1, queryBuilder.must().size());
        QueryBuilder idQuery = queryBuilder.must().get(0);
        assertTrue(idQuery.toString().contains(_ID_ONE));
    }

    public void testBuildGetRuleQuery_WithAttributes() {
        QueryBuilder queryBuilder = sut.buildQuery(
            new GetRuleRequest(null, ATTRIBUTE_MAP, null, RuleTestUtils.MockRuleFeatureType.INSTANCE)
        );
        assertNotNull(queryBuilder);
        BoolQueryBuilder query = (BoolQueryBuilder) queryBuilder;
        assertTrue(query.must().size() == 1);
        assertTrue(query.toString().contains(RuleTestUtils.MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE.getName()));
        assertTrue(query.toString().contains(ATTRIBUTE_VALUE_ONE));
    }
}
