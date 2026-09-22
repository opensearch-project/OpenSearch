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

import java.util.List;

/**
 * End-to-end get-by-id coverage for {@link DataFormatAwareReadOnlyEngine}: after an index is tiered to
 * warm, a document is still resolvable by id via the read-only row path (the warm engine has no version map).
 */
@AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/pull/21803")
public class DataFormatAwareReadonlyGetByIdIT extends DataFormatAwareReadonlyEngineBaseIT {

    public void testGetByIdFromWarmReadOnlyEngine() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        List<String> ids = createHotIndexAndTierToWarm(0); // indexes ids 0..49 (field_text="value_<i>", field_number=<i>), flush, flip to
                                                           // warm

        // Confirm the warm primary really runs the read-only engine.
        IndexShard primaryShard = getIndexShard(primaryNodeName());
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue(
            "warm primary must use DataFormatAwareReadOnlyEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareReadOnlyEngine
        );

        long cnt = 1;
        for (String id : ids) {
            GetResponse resp = client().prepareGet(INDEX_NAME, id).setRealtime(false).get();

            assertTrue("replica get-by-id must find the replicated doc via rows", resp.isExists());
            assertEquals(1L, resp.getVersion());
            assertEquals(cnt++, ((Number) resp.getSourceAsMap().get("field_number")).longValue());
            assertNotNull(resp.getSourceAsMap().get("field_text"));
            assertNotNull(resp.getSourceAsMap().get("field_keyword"));
        }
    }
}
