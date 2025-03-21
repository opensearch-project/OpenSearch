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

package org.opensearch.action.admin.cluster.snapshots.restore;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Map;

/**
 * Restore snapshot request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RestoreSnapshotRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    RestoreSnapshotRequest,
    RestoreSnapshotResponse,
    RestoreSnapshotRequestBuilder> {

    /**
     * Constructs new restore snapshot request builder
     */
    public RestoreSnapshotRequestBuilder(OpenSearchClient client, RestoreSnapshotAction action) {
        super(client, action, new RestoreSnapshotRequest());
    }

    /**
     * Constructs new restore snapshot request builder with specified repository and snapshot names
     */
    public RestoreSnapshotRequestBuilder(OpenSearchClient client, RestoreSnapshotAction action, String repository, String name) {
        super(client, action, new RestoreSnapshotRequest(repository, name));
    }

    /**
     * Sets snapshot name
     *
     * @param snapshot snapshot name
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setSnapshot(String snapshot) {
        request.snapshot(snapshot);
        return this;
    }

    /**
     * Sets repository name
     *
     * @param repository repository name
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

    /**
     * Sets the list of indices that should be restored from snapshot
     * <p>
     * The list of indices supports multi-index syntax. For example: "+test*" ,"-test42" will index all indices with
     * prefix "test" except index "test42". Aliases are not supported. An empty list or {"_all"} will restore all open
     * indices in the snapshot.
     *
     * @param indices list of indices
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Sets rename pattern that should be applied to restored indices.
     * <p>
     * Indices that match the rename pattern will be renamed according to {@link #setRenameReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more indices will be renamed into the same name.
     *
     * @param renamePattern rename pattern
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRenamePattern(String renamePattern) {
        request.renamePattern(renamePattern);
        return this;
    }

    /**
     * Sets rename replacement
     * <p>
     * See {@link #setRenamePattern(String)} for more information.
     *
     * @param renameReplacement rename replacement
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRenameReplacement(String renameReplacement) {
        request.renameReplacement(renameReplacement);
        return this;
    }

    /**
     * Sets rename pattern that should be applied to restored indices' aliases.
     * <p>
     * Aliases that match the rename pattern will be renamed according to {@link #setRenameAliasReplacement(String)}. The
     * rename pattern is applied according to the {@link java.util.regex.Matcher#appendReplacement(StringBuffer, String)}
     * The request will fail if two or more alias will be renamed into the same name.
     *
     * @param renameAliasPattern rename alias pattern
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRenameAliasPattern(String renameAliasPattern) {
        request.renameAliasPattern(renameAliasPattern);
        return this;
    }

    /**
     * Sets rename replacement
     * <p>
     * See {@link #setRenameAliasPattern(String)} for more information.
     *
     * @param renameAliasReplacement rename alias replacement
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRenameAliasReplacement(String renameAliasReplacement) {
        request.renameAliasReplacement(renameAliasReplacement);
        return this;
    }

    /**
     * If this parameter is set to true the operation will wait for completion of restore process before returning.
     *
     * @param waitForCompletion if true the operation will wait for completion
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * If set to true the restore procedure will restore global cluster state.
     * <p>
     * The global cluster state includes persistent settings and index template definitions.
     *
     * @param restoreGlobalState true if global state should be restored from the snapshot
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setRestoreGlobalState(boolean restoreGlobalState) {
        request.includeGlobalState(restoreGlobalState);
        return this;
    }

    /**
     * If set to true the restore procedure will restore partially snapshotted indices
     *
     * @param partial true if partially snapshotted indices should be restored
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setPartial(boolean partial) {
        request.partial(partial);
        return this;
    }

    /**
     * If set to true the restore procedure will restore aliases
     *
     * @param restoreAliases true if aliases should be restored from the snapshot
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIncludeAliases(boolean restoreAliases) {
        request.includeAliases(restoreAliases);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param settings index settings
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(Settings settings) {
        request.indexSettings(settings);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param settings index settings
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(Settings.Builder settings) {
        request.indexSettings(settings);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param source index settings
     * @param xContentType the content type of the source
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(String source, XContentType xContentType) {
        request.indexSettings(source, xContentType);
        return this;
    }

    /**
     * Sets index settings that should be added or replaced during restore
     *
     * @param source index settings
     * @return this builder
     */
    public RestoreSnapshotRequestBuilder setIndexSettings(Map<String, Object> source) {
        request.indexSettings(source);
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequestBuilder setIgnoreIndexSettings(String... ignoreIndexSettings) {
        request.ignoreIndexSettings(ignoreIndexSettings);
        return this;
    }

    /**
     * Sets the list of index settings and index settings groups that shouldn't be restored from snapshot
     */
    public RestoreSnapshotRequestBuilder setIgnoreIndexSettings(List<String> ignoreIndexSettings) {
        request.ignoreIndexSettings(ignoreIndexSettings);
        return this;
    }

    /**
     * Sets the storage type
     */
    public RestoreSnapshotRequestBuilder setStorageType(RestoreSnapshotRequest.StorageType storageType) {
        request.storageType(storageType);
        return this;
    }

    /**
     * Sets the source remote store repository name
     */
    public RestoreSnapshotRequestBuilder setSourceRemoteStoreRepository(String repositoryName) {
        request.setSourceRemoteStoreRepository(repositoryName);
        return this;
    }

    /**
     * Sets the source remote translog repository name
     */
    public RestoreSnapshotRequestBuilder setSourceRemoteTranslogRepository(String repositoryName) {
        request.setSourceRemoteTranslogRepository(repositoryName);
        return this;
    }
}
