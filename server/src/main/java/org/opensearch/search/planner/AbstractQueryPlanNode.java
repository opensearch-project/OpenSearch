/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.planner;

import org.apache.lucene.search.Query;

import java.util.Collections;
import java.util.List;

/**
 * Base implementation of QueryPlanNode providing common functionality.
 *
 * @opensearch.internal
 */
public abstract class AbstractQueryPlanNode implements QueryPlanNode {

    protected final Query luceneQuery;
    protected final QueryNodeType nodeType;
    protected final List<QueryPlanNode> children;
    protected volatile QueryCost estimatedCost;

    protected AbstractQueryPlanNode(Query luceneQuery, QueryNodeType nodeType) {
        this(luceneQuery, nodeType, Collections.emptyList());
    }

    protected AbstractQueryPlanNode(Query luceneQuery, QueryNodeType nodeType, List<QueryPlanNode> children) {
        this.luceneQuery = luceneQuery;
        this.nodeType = nodeType;
        this.children = Collections.unmodifiableList(children);
    }

    @Override
    public Query getLuceneQuery() {
        return luceneQuery;
    }

    @Override
    public QueryNodeType getType() {
        return nodeType;
    }

    @Override
    public List<QueryPlanNode> getChildren() {
        return children;
    }

    @Override
    public QueryCost getEstimatedCost() {
        QueryCost cost = estimatedCost;
        if (cost == null) {
            cost = calculateCost();
            estimatedCost = cost;
        }
        return cost;
    }

    /**
     * Calculate the cost for this node. Subclasses should implement
     * their specific cost calculation logic.
     */
    protected abstract QueryCost calculateCost();

    @Override
    public QueryPlanProfile getProfile() {
        QueryPlanProfile profile = new QueryPlanProfile(nodeType.name(), getEstimatedCost());
        addProfileAttributes(profile);
        return profile;
    }

    /**
     * Add node-specific attributes to the profile.
     * Subclasses can override to add custom attributes.
     */
    protected void addProfileAttributes(QueryPlanProfile profile) {
        profile.addAttribute("lucene_query", luceneQuery.toString());
        profile.addAttribute("children_count", children.size());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeType.name()).append("[");
        sb.append("query=").append(luceneQuery.getClass().getSimpleName());
        sb.append(", cost=").append(getEstimatedCost().getTotalCost());
        if (!children.isEmpty()) {
            sb.append(", children=").append(children.size());
        }
        sb.append("]");
        return sb.toString();
    }
}
