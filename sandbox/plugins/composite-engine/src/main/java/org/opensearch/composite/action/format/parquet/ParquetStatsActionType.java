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
 * ActionType definition for the parquet stats transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetStatsActionType extends ActionType<ParquetStatsResponse> {

    public static final String NAME = "indices:monitor/parquet/stats";
    public static final ParquetStatsActionType INSTANCE = new ParquetStatsActionType();

    private ParquetStatsActionType() {
        super(NAME, ParquetStatsResponse::new);
    }
}
