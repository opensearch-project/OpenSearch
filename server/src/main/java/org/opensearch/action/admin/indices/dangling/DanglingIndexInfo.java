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

package org.opensearch.action.admin.indices.dangling;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Contains information about a dangling index, i.e. an index that OpenSearch has found
 * on-disk but is not present in the cluster state.
 *
 * @opensearch.internal
 */
public class DanglingIndexInfo implements Writeable {
    private final String nodeId;
    private final String indexName;
    private final String indexUUID;
    private final long creationDateMillis;

    public DanglingIndexInfo(String nodeId, String indexName, String indexUUID, long creationDateMillis) {
        this.nodeId = nodeId;
        this.indexName = indexName;
        this.indexUUID = indexUUID;
        this.creationDateMillis = creationDateMillis;
    }

    public DanglingIndexInfo(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.indexName = in.readString();
        this.indexUUID = in.readString();
        this.creationDateMillis = in.readLong();
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public long getCreationDateMillis() {
        return creationDateMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.nodeId);
        out.writeString(this.indexName);
        out.writeString(this.indexUUID);
        out.writeLong(this.creationDateMillis);
    }
}
