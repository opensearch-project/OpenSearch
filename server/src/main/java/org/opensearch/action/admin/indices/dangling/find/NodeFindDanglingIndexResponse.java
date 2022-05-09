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

package org.opensearch.action.admin.indices.dangling.find;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Used when querying every node in the cluster for a specific dangling index.
 *
 * @opensearch.internal
 */
public class NodeFindDanglingIndexResponse extends BaseNodeResponse {
    /**
     * A node could report several dangling indices. This class will contain them all.
     * A single node could even multiple different index versions for the same index
     * UUID if the situation is really crazy, though perhaps this is more likely
     * when collating responses from different nodes.
     */
    private final List<IndexMetadata> danglingIndexInfo;

    public List<IndexMetadata> getDanglingIndexInfo() {
        return this.danglingIndexInfo;
    }

    public NodeFindDanglingIndexResponse(DiscoveryNode node, List<IndexMetadata> danglingIndexInfo) {
        super(node);
        this.danglingIndexInfo = danglingIndexInfo;
    }

    protected NodeFindDanglingIndexResponse(StreamInput in) throws IOException {
        super(in);
        this.danglingIndexInfo = in.readList(IndexMetadata::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(this.danglingIndexInfo);
    }
}
