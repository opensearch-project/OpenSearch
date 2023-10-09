/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.test.AbstractBuilderTestCase;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilderVisitorTests extends AbstractBuilderTestCase {

    public void testNoOpsVisitor() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        List<QueryBuilder> visitedQueries = new ArrayList<>();
        QueryBuilderVisitor qbv = createTestVisitor(visitedQueries);
        boolQueryBuilder.visit(qbv, 0);
        QueryBuilderVisitor subQbv = qbv.getChildVisitor(BooleanClause.Occur.MUST_NOT);
        assertEquals(0, visitedQueries.size());
        assertEquals(qbv, subQbv);
    }

    protected static QueryBuilderVisitor createTestVisitor(List<QueryBuilder> visitedQueries) {
        return QueryBuilderVisitor.NO_OP_VISITOR;
    }
}
