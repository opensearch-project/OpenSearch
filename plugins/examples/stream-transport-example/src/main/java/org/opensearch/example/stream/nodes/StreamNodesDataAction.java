/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream.nodes;

import org.opensearch.action.ActionType;

/**
 * Action for streaming data from all nodes
 */
/**
 * Example class
 */
/** Example */
public class StreamNodesDataAction extends ActionType<StreamNodesDataResponse> {
    /** Singleton instance */
    /** Method */
    public static final StreamNodesDataAction INSTANCE = new StreamNodesDataAction();
    /** Action name */
    /** Method */
    public static final String NAME = "cluster:monitor/stream_nodes_data";

    private StreamNodesDataAction() {
        super(NAME, StreamNodesDataResponse::new);
    }
}
