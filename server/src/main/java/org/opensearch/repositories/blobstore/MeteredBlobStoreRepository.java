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

package org.opensearch.repositories.blobstore;

import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.repositories.RepositoryInfo;
import org.opensearch.repositories.RepositoryStatsSnapshot;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;

/**
 * A blob store repository that is metered
 *
 * @opensearch.internal
 */
public abstract class MeteredBlobStoreRepository extends BlobStoreRepository {
    private final RepositoryInfo repositoryInfo;

    public MeteredBlobStoreRepository(
        RepositoryMetadata metadata,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        Map<String, String> location
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);
        ThreadPool threadPool = clusterService.getClusterApplierService().threadPool();
        this.repositoryInfo = new RepositoryInfo(
            UUIDs.randomBase64UUID(),
            metadata.name(),
            metadata.type(),
            location,
            threadPool.absoluteTimeInMillis()
        );
    }

    @Override
    public void reload(RepositoryMetadata repositoryMetadata) {
        super.reload(repositoryMetadata);

        // Not adding any additional reload logic here is intentional as the constructor only
        // initializes the repositoryInfo from the repo metadata, which cannot be changed.
    }

    public RepositoryStatsSnapshot statsSnapshot() {
        return new RepositoryStatsSnapshot(repositoryInfo, stats(), RepositoryStatsSnapshot.UNKNOWN_CLUSTER_VERSION, false);
    }

    public RepositoryStatsSnapshot statsSnapshotForArchival(long clusterVersion) {
        RepositoryInfo stoppedRepoInfo = repositoryInfo.stopped(threadPool.absoluteTimeInMillis());
        return new RepositoryStatsSnapshot(stoppedRepoInfo, stats(), clusterVersion, true);
    }
}
