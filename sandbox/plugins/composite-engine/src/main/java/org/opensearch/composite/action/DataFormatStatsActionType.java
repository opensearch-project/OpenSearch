/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.action;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * ActionType definition for the dataformat stats transport action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatStatsActionType extends ActionType<DataFormatStatsResponse> {

    public static final String NAME = "indices:monitor/dataformat/stats";
    public static final DataFormatStatsActionType INSTANCE = new DataFormatStatsActionType();

    private DataFormatStatsActionType() {
        super(NAME, DataFormatStatsResponse::new);
    }
}
