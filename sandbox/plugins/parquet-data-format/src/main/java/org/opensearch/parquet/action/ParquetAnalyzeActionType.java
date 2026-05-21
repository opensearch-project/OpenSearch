/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * ActionType definition for the parquet analyze transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetAnalyzeActionType extends ActionType<ParquetAnalyzeResponse> {

    public static final String NAME = "indices:monitor/parquet/analyze";
    public static final ParquetAnalyzeActionType INSTANCE = new ParquetAnalyzeActionType();

    private ParquetAnalyzeActionType() {
        super(NAME, ParquetAnalyzeResponse::new);
    }
}
