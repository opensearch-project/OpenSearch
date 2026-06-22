/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.action.get.GetResponse;
import org.opensearch.index.engine.DataFormatAwareReadOnlyEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;

/**
 * End-to-end get-by-id coverage for {@link DataFormatAwareReadOnlyEngine}: after an index is tiered to
 * warm, a document is still resolvable by id via the read-only row path (the warm engine has no version map).
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21803")
public class DataFormatAwareReadonlyGetByIdIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testGetByIdFromWarmReadOnlyEngine() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWarm(0); // indexes ids 0..49 (field_text="value_<i>", field_number=<i>), flush, flip to warm

        // Confirm the warm primary really runs the read-only engine.
        IndexShard primaryShard = getIndexShard(primaryNodeName());
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue(
            "warm primary must use DataFormatAwareReadOnlyEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareReadOnlyEngine
        );

        // GET by id resolves via the warm read-only row path.
        GetResponse resp = client().prepareGet(INDEX_NAME, "5").setRealtime(false).get();
        assertTrue("warm get-by-id must find the doc via rows", resp.isExists());
        assertEquals(1L, resp.getVersion());
        assertEquals("value_5", resp.getSourceAsMap().get("field_text"));
        assertEquals(5L, ((Number) resp.getSourceAsMap().get("field_number")).longValue());
    }
}
