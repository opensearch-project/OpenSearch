/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.block;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.test.OpenSearchIntegTestCase.client;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;

public class CreateIndexBlockIT extends OpenSearchIntegTestCase {

    public void testBlockCreateIndex() {
        try {
            setCreateIndexBlock("true");
            assertBlocked(client().admin().indices().prepareCreate("diskguardrails1"),
                Metadata.CLUSTER_CREATE_INDEX_BLOCK);
        } finally {
            setCreateIndexBlock("false");
            assertAcked(client().admin().indices().prepareCreate("diskguardrails2").execute().actionGet());
        }

    }

    private void setCreateIndexBlock(String value) {
        Settings settings = Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK.getKey(), value).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }

}
