/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats.transport;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for per-format node-level stats.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FormatNodeStatsRequest extends BaseNodesRequest<FormatNodeStatsRequest> {

    private final String formatName;

    public FormatNodeStatsRequest(String formatName, String... nodeIds) {
        super(nodeIds);
        this.formatName = formatName;
    }

    public FormatNodeStatsRequest(StreamInput in) throws IOException {
        super(in);
        this.formatName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(formatName);
    }

    public String formatName() {
        return formatName;
    }
}
