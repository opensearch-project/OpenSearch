/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.profile;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Query-level profile snapshot built from an execution graph plus the per-query
 * {@code TaskTracker}. Safe to emit on both success and failure paths — every field
 * is a plain value captured at snapshot time, not a live handle into the walker.
 *
 * @param queryId        per-query id from {@code QueryDAG.queryId()}
 * @param fullPlan       the CBO-output Calcite plan rendered as an array of lines,
 *                       captured before the DAG builder cut it at exchange boundaries;
 *                       one element per indent level of the tree. Empty list if not supplied.
 * @param totalElapsedMs wall-clock span from the earliest stage start to the latest stage end (0 if nothing ran)
 * @param stages         per-stage profiles in DAG iteration order (root stage appears at whatever index the walker stored it)
 */
public record QueryProfile(String queryId, List<String> fullPlan, long totalElapsedMs, List<StageProfile> stages)
    implements
        ToXContentObject {

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("query_id", queryId);
        if (fullPlan != null && fullPlan.isEmpty() == false) {
            builder.startArray("full_plan");
            for (String line : fullPlan)
                builder.value(line);
            builder.endArray();
        }
        builder.field("total_elapsed_ms", totalElapsedMs);
        builder.startArray("stages");
        for (StageProfile s : stages) {
            s.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
