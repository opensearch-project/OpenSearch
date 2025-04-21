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
import org.apache.lucene.index.LeafReader;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Implementation of a {@link IndexWriter.IndexReaderWarmer} when remote store is enabled.
 *
 * @opensearch.internal
 */
public class RemoteStoreMergedSegmentWarmer implements IndexWriter.IndexReaderWarmer {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    public RemoteStoreMergedSegmentWarmer(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    @Override
    public void warm(LeafReader leafReader) throws IOException {
        // TODO: remote store merged segment warmer
    }
}
