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

package org.opensearch.indices.state;

import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.close.TransportCloseIndexAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CloseIndexDisableCloseAllIT extends OpenSearchIntegTestCase {

    @After
    public void afterTest() {
        Settings settings = Settings.builder()
            .put(TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey(), (String) null)
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
    }

    public void testCloseAllRequiresName() {
        createIndex("test1", "test2", "test3");

        assertAcked(client().admin().indices().prepareClose("test3", "test2"));
        assertIndexIsClosed("test2", "test3");

        // disable closing
        createIndex("test_no_close");
        Settings settings = Settings.builder().put(TransportCloseIndexAction.CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey(), false).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        IllegalStateException illegalStateException = expectThrows(
            IllegalStateException.class,
            () -> client().admin().indices().prepareClose("test_no_close").get()
        );
        assertEquals(
            illegalStateException.getMessage(),
            "closing indices is disabled - set [cluster.indices.close.enable: true] to enable it. NOTE: closed indices still "
                + "consume a significant amount of diskspace"
        );
    }

    private void assertIndexIsClosed(String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetadata indexMetadata = clusterStateResponse.getState().metadata().indices().get(index);
            assertNotNull(indexMetadata);
            assertEquals(IndexMetadata.State.CLOSE, indexMetadata.getState());
        }
    }
}
