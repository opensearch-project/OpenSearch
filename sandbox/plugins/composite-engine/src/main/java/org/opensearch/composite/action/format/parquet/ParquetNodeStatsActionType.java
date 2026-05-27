/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action.format.parquet;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * ActionType definition for the parquet node stats transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetNodeStatsActionType extends ActionType<ParquetNodeStatsResponse> {

    public static final String NAME = "cluster:monitor/parquet/nodes/stats";
    public static final ParquetNodeStatsActionType INSTANCE = new ParquetNodeStatsActionType();

    private ParquetNodeStatsActionType() {
        super(NAME, ParquetNodeStatsResponse::new);
    }
}
