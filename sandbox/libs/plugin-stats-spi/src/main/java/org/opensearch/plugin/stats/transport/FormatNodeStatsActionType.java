/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.ActionType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.plugin.stats.DataFormatShardStats;

/**
 * ActionType for per-format node-level stats, parameterised by the format's stats type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatNodeStatsActionType<T extends DataFormatShardStats<T>> extends ActionType<FormatNodeStatsResponse<T>> {

    public FormatNodeStatsActionType(String formatName, Writeable.Reader<T> reader) {
        super("cluster:monitor/dfa/" + formatName + "/nodes/stats", in -> new FormatNodeStatsResponse<>(in, reader));
    }
}
