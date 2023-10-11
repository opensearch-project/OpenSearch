/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.SetOnce;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Class to traverse the QueryBuilder tree and capture the query shape
 */
public class QueryShapeVisitor implements QueryBuilderVisitor {
    private final SetOnce<String> queryType = new SetOnce<>();
    private final Map<BooleanClause.Occur, List<QueryShapeVisitor>> childVisitors = new EnumMap<>(BooleanClause.Occur.class);

    @Override
    public void accept(QueryBuilder qb) {
        queryType.set(qb.getName());
    }

    @Override
    public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
        // Should get called once per Occur value
        if (childVisitors.containsKey(occur)) {
            throw new IllegalStateException("getChildVisitor already called for " + occur);
        }
        final List<QueryShapeVisitor> childVisitorList = new ArrayList<>();
        QueryBuilderVisitor childVisitorWrapper = new QueryBuilderVisitor() {
            QueryShapeVisitor currentChild;

            @Override
            public void accept(QueryBuilder qb) {
                currentChild = new QueryShapeVisitor();
                childVisitorList.add(currentChild);
                currentChild.accept(qb);
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return currentChild.getChildVisitor(occur);
            }
        };
        childVisitors.put(occur, childVisitorList);
        return childVisitorWrapper;
    }

    public String toJson() {
        StringBuilder outputBuilder = new StringBuilder("{\"type\":\"").append(queryType.get()).append("\"");
        for (Map.Entry<BooleanClause.Occur, List<QueryShapeVisitor>> entry : childVisitors.entrySet()) {
            outputBuilder.append(",\"").append(entry.getKey().name().toLowerCase(Locale.ROOT)).append("\"[");
            boolean first = true;
            for (QueryShapeVisitor child : entry.getValue()) {
                if (!first) {
                    outputBuilder.append(",");
                }
                outputBuilder.append(child.toJson());
                first = false;
            }
            outputBuilder.append("]");
        }
        outputBuilder.append("}");
        return outputBuilder.toString();
    }

    public String prettyPrintTree(String indent) {
        StringBuilder outputBuilder = new StringBuilder(indent).append(queryType.get()).append("\n");
        for (Map.Entry<BooleanClause.Occur, List<QueryShapeVisitor>> entry : childVisitors.entrySet()) {
            outputBuilder.append(indent).append("  ").append(entry.getKey().name().toLowerCase(Locale.ROOT)).append(":\n");
            for (QueryShapeVisitor child : entry.getValue()) {
                outputBuilder.append(child.prettyPrintTree(indent + "    "));
            }
        }
        return outputBuilder.toString();
    }
}
