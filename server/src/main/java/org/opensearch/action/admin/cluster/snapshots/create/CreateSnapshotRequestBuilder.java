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

package org.opensearch.action.admin.cluster.snapshots.create;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.transport.client.OpenSearchClient;

import java.util.Map;

/**
 * Create snapshot request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CreateSnapshotRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    CreateSnapshotRequest,
    CreateSnapshotResponse,
    CreateSnapshotRequestBuilder> {

    /**
     * Constructs a new create snapshot request builder
     */
    public CreateSnapshotRequestBuilder(OpenSearchClient client, CreateSnapshotAction action) {
        super(client, action, new CreateSnapshotRequest());
    }

    /**
     * Constructs a new create snapshot request builder with specified repository and snapshot names
     */
    public CreateSnapshotRequestBuilder(OpenSearchClient client, CreateSnapshotAction action, String repository, String snapshot) {
        super(client, action, new CreateSnapshotRequest(repository, snapshot));
    }

    /**
     * Sets the snapshot name
     *
     * @param snapshot snapshot name
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSnapshot(String snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    /**
     * Sets the repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets a list of indices that should be included into the snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are supported. An empty list or {"_all"} will snapshot all open
     * indices in the cluster.
     *
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateSnapshotRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * If set to true the request should wait for the snapshot completion before returning.
     *
     * @param waitForCompletion true if
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * If set to true the request should snapshot indices with unavailable shards
     *
     * @param partial true if request should snapshot indices with unavailable shards
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setPartial(boolean partial) {
        request.partial(partial);
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets repository-specific snapshot settings in YAML or JSON format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific snapshot settings
     * @param xContentType the content type of the source
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets repository-specific snapshot settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific snapshot settings
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setSettings(Map<String, Object> settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Set to true if snapshot should include global cluster state
     *
     * @param includeGlobalState true if snapshot should include global cluster state
     * @return this builder
     */
    public CreateSnapshotRequestBuilder setIncludeGlobalState(boolean includeGlobalState) {
        request.includeGlobalState(includeGlobalState);
        return this;
    }
}
