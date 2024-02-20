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

package org.opensearch.action.admin.indices.refresh;

import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Arrays;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_WRITE_ONLY_ALLOW_DELETE;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RefreshBlocksIT extends OpenSearchIntegTestCase {
    public void testRefreshWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        NumShards numShards = getNumShards("test");

        // Request is not blocked
        for (String blockSetting : Arrays.asList(
            SETTING_BLOCKS_READ,
            SETTING_BLOCKS_WRITE,
            SETTING_READ_ONLY,
            SETTING_BLOCKS_METADATA,
            SETTING_READ_ONLY_ALLOW_DELETE,
            SETTING_WRITE_ONLY_ALLOW_DELETE
        )) {
            try {
                enableIndexBlock("test", blockSetting);
                RefreshResponse response = client().admin().indices().prepareRefresh("test").execute().actionGet();
                assertNoFailures(response);
                assertThat(response.getSuccessfulShards(), equalTo(numShards.totalNumShards));
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }
}
