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
 * ActionType for per-format index-level stats, parameterised by the format's stats type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatStatsActionType<T extends DataFormatShardStats<T>> extends ActionType<FormatStatsResponse<T>> {

    public FormatStatsActionType(String formatName, Writeable.Reader<T> reader) {
        super("cluster:monitor/dfa/" + formatName + "/index/stats", in -> new FormatStatsResponse<>(in, reader));
    }
}
