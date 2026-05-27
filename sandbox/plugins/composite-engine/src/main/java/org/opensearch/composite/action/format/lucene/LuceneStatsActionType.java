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
 * ActionType definition for the Lucene index-level stats transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneStatsActionType extends ActionType<LuceneStatsResponse> {

    public static final String NAME = "indices:monitor/lucene/stats";
    public static final LuceneStatsActionType INSTANCE = new LuceneStatsActionType();

    private LuceneStatsActionType() {
        super(NAME, LuceneStatsResponse::new);
    }
}
