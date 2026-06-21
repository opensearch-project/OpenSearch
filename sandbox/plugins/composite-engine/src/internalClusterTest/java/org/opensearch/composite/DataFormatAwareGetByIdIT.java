/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * End-to-end get-by-id coverage for the hot {@link org.opensearch.index.engine.DataFormatAwareEngine}:
 * exercises both the in-memory version-map path (realtime GET before refresh) and the parquet row
 * path (GET after refresh) under active indexing and interleaved refreshes.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DataFormatAwareGetByIdIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "dfae_getbyid";

    /**
     * Creates the composite index (Lucene secondary for {@code _id} resolution) with auto-refresh
     * disabled so the version-map vs row paths stay deterministic across the refresh boundary.
     */
    private void createManualRefreshIndex() {
        createCompositeIndex(INDEX); // withLuceneSecondary = true
        client().admin().indices().prepareUpdateSettings(INDEX).setSettings(Settings.builder().put("index.refresh_interval", -1)).get();
    }

    private IndexResponse indexDoc(String id, String name, int value) {
        return client().prepareIndex().setIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
    }

    private static int intValue(GetResponse r) {
        return ((Number) r.getSourceAsMap().get("value")).intValue();
    }

    public void testRealtimeGetHitsVersionMapBeforeRefresh() {
        createManualRefreshIndex();
        assertEquals(RestStatus.CREATED, indexDoc("1", "doc_1", 1).status());

        // Realtime GET resolves from the in-memory version map (translog-backed) before any refresh.
        GetResponse realtime = client().prepareGet(INDEX, "1").setRealtime(true).get();
        assertTrue("realtime get must find the unrefreshed doc", realtime.isExists());
        assertEquals(1L, realtime.getVersion());
        assertEquals("doc_1", realtime.getSourceAsMap().get("name"));
        assertEquals(1, intValue(realtime));

        // Non-realtime GET only sees refreshed (row) data -> not found yet, proving rows are still empty.
        GetResponse nonRealtime = client().prepareGet(INDEX, "1").setRealtime(false).get();
        assertFalse("non-realtime get must not see the unrefreshed doc", nonRealtime.isExists());
    }

    public void testGetHitsRowsAfterRefresh() {
        createManualRefreshIndex();
        assertEquals(RestStatus.CREATED, indexDoc("2", "doc_2", 2).status());
        refreshIndex(INDEX);

        // After refresh the doc is materialized into parquet rows; non-realtime GET resolves via the row path.
        GetResponse resp = client().prepareGet(INDEX, "2").setRealtime(false).get();
        assertTrue("post-refresh get must find the doc via rows", resp.isExists());
        assertEquals(1L, resp.getVersion());
        assertEquals("doc_2", resp.getSourceAsMap().get("name"));
        assertEquals(2, intValue(resp));
    }

    public void testActiveIndexingWithInterleavedRefreshes() {
        createManualRefreshIndex();
        // First batch then refresh -> these live in rows.
        indexDocs(INDEX, 25, 1);
        refreshIndex(INDEX);
        // Second batch, NOT refreshed -> these live only in the version map.
        indexDocs(INDEX, 25, 26);

        // Refreshed id -> row path.
        GetResponse refreshed = client().prepareGet(INDEX, "10").setRealtime(false).get();
        assertTrue("refreshed id must be found via rows", refreshed.isExists());
        assertEquals("doc_10", refreshed.getSourceAsMap().get("name"));
        assertEquals(10, intValue(refreshed));

        // Unrefreshed id -> version-map path (realtime found), absent from rows (non-realtime not found).
        GetResponse unrefreshedRealtime = client().prepareGet(INDEX, "40").setRealtime(true).get();
        assertTrue("unrefreshed id must be found realtime via version map", unrefreshedRealtime.isExists());
        assertEquals("doc_40", unrefreshedRealtime.getSourceAsMap().get("name"));
        assertFalse("unrefreshed id must be absent from rows", client().prepareGet(INDEX, "40").setRealtime(false).get().isExists());

        // After a second refresh the previously-unrefreshed id resolves via rows too.
        refreshIndex(INDEX);
        GetResponse nowInRows = client().prepareGet(INDEX, "40").setRealtime(false).get();
        assertTrue("after refresh id must be found via rows", nowInRows.isExists());
        assertEquals(40, intValue(nowInRows));
    }
}
