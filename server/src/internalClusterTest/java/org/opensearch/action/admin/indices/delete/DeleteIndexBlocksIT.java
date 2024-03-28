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

package org.opensearch.action.admin.indices.delete;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;

public class DeleteIndexBlocksIT extends OpenSearchIntegTestCase {
    public void testDeleteIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().prepareDelete("test"), Metadata.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testDeleteIndexOnIndexReadOnlyAllowDeleteSetting() {
        assertDeleteIndexOnAllowDeleteSetting(
            IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE,
            IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
        );
    }

    public void testClusterBlockMessageHasIndexName() {
        try {
            createIndex("test");
            ensureGreen("test");
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build();
            client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
            ClusterBlockException e = expectThrows(
                ClusterBlockException.class,
                () -> client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get()
            );
            assertEquals(
                "index [test] blocked by: [TOO_MANY_REQUESTS/12/disk usage exceeded flood-stage watermark, "
                    + "index has read-only-allow-delete block];",
                e.getMessage()
            );
        } finally {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build())
                    .get()
            );
        }
    }

    public void testDeleteIndexOnClusterReadOnlyAllowDeleteSetting() {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get();
        refresh();
        try {
            Settings settings = Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
            assertSearchHits(client().prepareSearch().get(), "1");
            assertBlocked(
                client().prepareIndex().setIndex("test").setId("2").setSource("foo", "bar"),
                Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK
            );
            assertBlocked(
                client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.number_of_replicas", 2)),
                Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK
            );
            assertSearchHits(client().prepareSearch().get(), "1");
            assertAcked(client().admin().indices().prepareDelete("test"));
        } finally {
            Settings settings = Settings.builder().putNull(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
        }
    }

    public void testDeleteIndexOnIndexWriteOnlyAllowDeleteSetting() {
        assertDeleteIndexOnAllowDeleteSetting(
            IndexMetadata.SETTING_WRITE_ONLY_ALLOW_DELETE,
            IndexMetadata.INDEX_WRITE_ONLY_ALLOW_DELETE_BLOCK
        );
    }

    private void assertDeleteIndexOnAllowDeleteSetting(String settingName, ClusterBlock blockToAssert) {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get();
        refresh();
        try {
            Settings settings = Settings.builder().put(settingName, true).build();
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get());
            assertSearchHits(client().prepareSearch().get(), "1");
            assertBlocked(client().prepareIndex().setIndex("test").setId("2").setSource("foo", "bar"), blockToAssert);
            assertBlocked(
                client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.number_of_replicas", 2)),
                blockToAssert
            );
            assertSearchHits(client().prepareSearch().get(), "1");
            assertAcked(client().admin().indices().prepareDelete("test"));
        } finally {
            Settings settings = Settings.builder().putNull(settingName).build();
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings("test")
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setSettings(settings)
                    .get()
            );
        }
    }
}
