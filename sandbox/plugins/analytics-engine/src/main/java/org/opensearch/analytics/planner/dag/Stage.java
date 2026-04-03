/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a plan fragment (the marked RelNode
 * subtree between exchange boundaries) and references to child stages.
 *
 * <p>Fragment may be null for a pure gather stage (coordinator just accumulates
 * Arrow batches from child stages).
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;

    // TODO: add List<StagePlan> planAlternatives — populated during plan forking phase

    public Stage(int stageId, RelNode fragment, List<Stage> childStages, ExchangeInfo exchangeInfo) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
    }

    public int getStageId() {
        return stageId;
    }

    /** Marked plan fragment with annotations intact. Null for a pure gather stage. */
    public RelNode getFragment() {
        return fragment;
    }

    public List<Stage> getChildStages() {
        return childStages;
    }

    /** How this stage connects to its parent. Null for the root stage. */
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }
}
