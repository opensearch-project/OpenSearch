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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

/**
 * MergedSegmentWarmerFactory to enable creation of various local on-disk
 * and remote store flavors of {@link IndexWriter.IndexReaderWarmer}
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MergedSegmentWarmerFactory {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    public MergedSegmentWarmerFactory(TransportService transportService, RecoverySettings recoverySettings, ClusterService clusterService) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public IndexWriter.IndexReaderWarmer get(IndexShard shard) {
        if (shard.indexSettings().isAssignedOnRemoteNode()) {
            return new RemoteStoreMergedSegmentWarmer(transportService, recoverySettings, clusterService);
        } else if (shard.indexSettings().isSegRepLocalEnabled()) {
            return new LocalMergedSegmentWarmer(transportService, recoverySettings, clusterService);
        } else if (shard.indexSettings().isDocumentReplication()) {
            // MergedSegmentWarmerFactory#get is called when IndexShard is initialized. In scenario document replication,
            // IndexWriter.IndexReaderWarmer should be null.
            return null;
        }
        // We just handle known cases and throw exception at the last. This will allow predictability on the IndexReaderWarmer behaviour.
        throw new IllegalStateException(shard.shardId() + " can't determine IndexReaderWarmer");
    }
}
