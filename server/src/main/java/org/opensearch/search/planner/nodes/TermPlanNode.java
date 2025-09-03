/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner.nodes;

import org.apache.lucene.search.Query;
import org.opensearch.search.planner.AbstractQueryPlanNode;
import org.opensearch.search.planner.QueryCost;
import org.opensearch.search.planner.QueryNodeType;
import org.opensearch.search.planner.QueryPlanProfile;

/**
 * Plan node for term queries - exact match on a field.
 *
 * @opensearch.internal
 */
public class TermPlanNode extends AbstractQueryPlanNode {

    private final String field;
    private final Object value;
    private final long estimatedDocFrequency;

    public TermPlanNode(Query query, String field, Object value, long estimatedDocFrequency) {
        super(query, QueryNodeType.TERM);
        this.field = field;
        this.value = value;
        this.estimatedDocFrequency = estimatedDocFrequency;
    }

    @Override
    protected QueryCost calculateCost() {
        // Term queries are very efficient - direct index lookup
        // Base CPU cost is minimal
        double cpuCost = 0.001;

        // Memory cost is proportional to the posting list size
        long memoryCost = estimatedDocFrequency * 8; // Rough estimate: 8 bytes per doc ID

        // I/O cost is minimal for term queries (single index lookup)
        double ioCost = 0.01;

        return new QueryCost(estimatedDocFrequency, cpuCost, memoryCost, ioCost, true);
    }

    @Override
    protected void addProfileAttributes(QueryPlanProfile profile) {
        super.addProfileAttributes(profile);
        profile.addAttribute("field", field);
        profile.addAttribute("value", value.toString());
        profile.addAttribute("estimated_doc_frequency", estimatedDocFrequency);
    }

    public String getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }
}
