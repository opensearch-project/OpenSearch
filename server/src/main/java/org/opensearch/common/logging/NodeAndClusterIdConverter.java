/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.logging;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.opensearch.common.SetOnce;

import java.util.Arrays;

/**
 * Pattern converter to format the node_and_cluster_id{subfield} variable into fields. Possible subfields:
 * <code>node_id</code> and <code>cluster_uuid</code>
 * Keeping those two fields together assures that they will be atomically set and become visible in logs at the same time.
 *
 * @opensearch.internal
 */
@Plugin(category = PatternConverter.CATEGORY, name = "NodeAndClusterIdConverter")
@ConverterKeys({ "node_and_cluster_id" })
public final class NodeAndClusterIdConverter extends LogEventPatternConverter {
    private static final SetOnce<String> nodeId = new SetOnce<>();
    private static final SetOnce<String> clusterUuid = new SetOnce<>();

    /**
     * Called by log4j2 to initialize this converter.
     */
    public static NodeAndClusterIdConverter newInstance(@SuppressWarnings("unused") final String[] options) {
        return new NodeAndClusterIdConverter(options);
    }

    private final String[] options;

    public NodeAndClusterIdConverter(String[] options) {
        super("NodeAndClusterId", "node_and_cluster_id");
        this.options = options;
    }

    /**
     * Updates only once the clusterID and nodeId.
     * Subsequent executions will throw {@link org.apache.lucene.util.SetOnce.AlreadySetException}.
     *
     * @param nodeId      a nodeId received from cluster state update
     * @param clusterUUID a clusterId received from cluster state update
     */
    public static void setNodeIdAndClusterId(String nodeId, String clusterUUID) {
        NodeAndClusterIdConverter.nodeId.set(nodeId);
        NodeAndClusterIdConverter.clusterUuid.set(clusterUUID);
    }

    /**
     * Formats the node.id and cluster.uuid into json fields.
     *
     * @param event - a log event is ignored in this method as it uses the nodeId and clusterId to format
     */
    @Override
    public void format(LogEvent event, StringBuilder toAppendTo) {
        if (options == null || Arrays.stream(options).anyMatch(a -> "node_id".equals(a))) {
            if (nodeId.get() != null) {
                toAppendTo.append(nodeId.get());
            }
        } else {
            if (clusterUuid.get() != null) {
                toAppendTo.append(clusterUuid.get());
            }
        }
    }
}
