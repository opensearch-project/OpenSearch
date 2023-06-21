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

package org.opensearch.action.admin.indices.cache.clear;

import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collections;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ClearIndicesCacheBlocksIT extends OpenSearchIntegTestCase {
    public void testClearIndicesCacheWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test", blockSetting);
                ClearIndicesCacheResponse clearIndicesCacheResponse = client().admin()
                    .indices()
                    .prepareClearCache("test")
                    .setFieldDataCache(true)
                    .setQueryCache(true)
                    .setFieldDataCache(true)
                    .execute()
                    .actionGet();
                assertNoFailures(clearIndicesCacheResponse);
                assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
        // Request is blocked
        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(
                    client().admin().indices().prepareClearCache("test").setFieldDataCache(true).setQueryCache(true).setFieldDataCache(true)
                );
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testClearIndicesFileCacheWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_READ_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test", blockSetting);
                ClearIndicesCacheResponse clearIndicesCacheResponse = client().admin()
                    .indices()
                    .prepareClearCache("test")
                    .setFileCache(true)
                    .execute()
                    .actionGet();
                assertNoFailures(clearIndicesCacheResponse);
                assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        for (String blockSetting : Collections.singletonList(SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareClearCache("test").setQueryCache(true).setFileCache(true));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }
}
