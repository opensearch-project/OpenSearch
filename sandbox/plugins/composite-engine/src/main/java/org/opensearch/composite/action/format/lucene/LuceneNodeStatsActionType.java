/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.lucene;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * ActionType definition for the Lucene node-level stats transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneNodeStatsActionType extends ActionType<LuceneNodeStatsResponse> {

    public static final String NAME = "cluster:monitor/lucene/nodes/stats";
    public static final LuceneNodeStatsActionType INSTANCE = new LuceneNodeStatsActionType();

    private LuceneNodeStatsActionType() {
        super(NAME, LuceneNodeStatsResponse::new);
    }
}
