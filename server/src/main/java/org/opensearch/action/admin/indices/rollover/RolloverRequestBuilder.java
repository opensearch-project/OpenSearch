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

package org.opensearch.action.admin.indices.rollover;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeOperationRequestBuilder;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.transport.client.OpenSearchClient;

/**
 * Transport request to rollover an index.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RolloverRequestBuilder extends ClusterManagerNodeOperationRequestBuilder<
    RolloverRequest,
    RolloverResponse,
    RolloverRequestBuilder> {
    public RolloverRequestBuilder(OpenSearchClient client, RolloverAction action) {
        super(client, action, new RolloverRequest());
    }

    public RolloverRequestBuilder setRolloverTarget(String rolloverTarget) {
        this.request.setRolloverTarget(rolloverTarget);
        return this;
    }

    public RolloverRequestBuilder setNewIndexName(String newIndexName) {
        this.request.setNewIndexName(newIndexName);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexAgeCondition(TimeValue age) {
        this.request.addMaxIndexAgeCondition(age);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexDocsCondition(long docs) {
        this.request.addMaxIndexDocsCondition(docs);
        return this;
    }

    public RolloverRequestBuilder addMaxIndexSizeCondition(ByteSizeValue size) {
        this.request.addMaxIndexSizeCondition(size);
        return this;
    }

    public RolloverRequestBuilder dryRun(boolean dryRun) {
        this.request.dryRun(dryRun);
        return this;
    }

    public RolloverRequestBuilder settings(Settings settings) {
        this.request.getCreateIndexRequest().settings(settings);
        return this;
    }

    public RolloverRequestBuilder alias(Alias alias) {
        this.request.getCreateIndexRequest().alias(alias);
        return this;
    }

    public RolloverRequestBuilder simpleMapping(String... source) {
        this.request.getCreateIndexRequest().simpleMapping(source);
        return this;
    }

    /**
     * Sets the number of shard copies that should be active for creation of the
     * new rollover index to return. Defaults to {@link ActiveShardCount#DEFAULT}, which will
     * wait for one shard copy (the primary) to become active. Set this value to
     * {@link ActiveShardCount#ALL} to wait for all shards (primary and all replicas) to be active
     * before returning. Otherwise, use {@link ActiveShardCount#from(int)} to set this value to any
     * non-negative integer, up to the number of copies per shard (number of replicas + 1),
     * to wait for the desired amount of shard copies to become active before returning.
     * Index creation will only wait up until the timeout value for the number of shard copies
     * to be active before returning.  Check {@link RolloverResponse#isShardsAcknowledged()} to
     * determine if the requisite shard copies were all started before returning or timing out.
     *
     * @param waitForActiveShards number of active shard copies to wait on
     */
    public RolloverRequestBuilder waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.request.getCreateIndexRequest().waitForActiveShards(waitForActiveShards);
        return this;
    }

    /**
     * A shortcut for {@link #waitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public RolloverRequestBuilder waitForActiveShards(final int waitForActiveShards) {
        return waitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }
}
