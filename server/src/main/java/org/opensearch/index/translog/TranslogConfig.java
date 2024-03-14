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

package org.opensearch.index.translog;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;

import java.nio.file.Path;

/**
 * Holds all the configuration that is used to create a {@link Translog}.
 * Once {@link Translog} has been created with this object, changes to this
 * object will affect the {@link Translog} instance.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class TranslogConfig {

    public static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(1, ByteSizeUnit.MB);
    public static final ByteSizeValue EMPTY_TRANSLOG_BUFFER_SIZE = new ByteSizeValue(10, ByteSizeUnit.BYTES);
    private final BigArrays bigArrays;
    private final IndexSettings indexSettings;
    private final ShardId shardId;
    private final Path translogPath;
    private final ByteSizeValue bufferSize;
    private final String nodeId;

    /**
     * Creates a new TranslogConfig instance
     * @param shardId the shard ID this translog belongs to
     * @param translogPath the path to use for the transaction log files
     * @param indexSettings the index settings used to set internal variables
     * @param bigArrays a bigArrays instance used for temporarily allocating write operations
     */
    public TranslogConfig(ShardId shardId, Path translogPath, IndexSettings indexSettings, BigArrays bigArrays, String nodeId) {
        this(shardId, translogPath, indexSettings, bigArrays, DEFAULT_BUFFER_SIZE, nodeId);
    }

    TranslogConfig(
        ShardId shardId,
        Path translogPath,
        IndexSettings indexSettings,
        BigArrays bigArrays,
        ByteSizeValue bufferSize,
        String nodeId
    ) {
        this.bufferSize = bufferSize;
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.translogPath = translogPath;
        this.bigArrays = bigArrays;
        this.nodeId = nodeId;
    }

    /**
     * Returns the index indexSettings
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Returns the shard ID this config is created for
     */
    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns a BigArrays instance for this engine
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * Returns the translog path for this engine
     */
    public Path getTranslogPath() {
        return translogPath;
    }

    /**
     * The translog buffer size. Default is {@code 8kb}
     */
    public ByteSizeValue getBufferSize() {
        return bufferSize;
    }

    public String getNodeId() {
        return nodeId;
    }
}
