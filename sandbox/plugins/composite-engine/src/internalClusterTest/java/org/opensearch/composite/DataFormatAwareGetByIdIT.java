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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

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

    private IndexResponse indexDoc(String name, int value) {
        return client().prepareIndex().setIndex(INDEX).setSource("name", name, "value", value).get();
    }

    private static int intValue(GetResponse r) {
        return ((Number) r.getSourceAsMap().get("value")).intValue();
    }

    /**
     * Composite index that accepts custom document ids, so a document can be re-indexed and its
     * {@code _version} advanced. {@code createCompositeIndex} leaves {@code append_only.enabled}
     * on, which rejects custom ids outright.
     */
    private void createUpdatableManualRefreshIndex() {
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
                    .put("index.refresh_interval", -1)
            )
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    /** Indexes under an explicit id so successive calls advance {@code _version}. */
    private IndexResponse indexDocWithId(String id, String name, int value) {
        return client().prepareIndex().setIndex(INDEX).setId(id).setSource("name", name, "value", value).get();
    }

    /**
     * A GET naming an explicit {@code version} must be rejected once the document has been
     * refreshed out of the live version map.
     *
     * <p>{@code RestGetAction} parses {@code version} and {@code version_type} and
     * {@code ShardGetService} threads them into {@code Engine.Get}, so the precondition reaches the
     * engine. The version-map branch checks it inline; the segment fall-through used to skip the
     * check and return the <em>current</em> document with a 200 for a request that named a
     * different version — so the condition was not merely unenforced, it was invisible.
     *
     * <p>Exercised through the transport layer on purpose: the parsing above sits outside the
     * engine, so an engine-level test cannot cover it.
     */
    public void testGetWithStaleVersionAfterRefreshConflicts() {
        createUpdatableManualRefreshIndex();

        assertEquals(1L, indexDocWithId("v1", "first", 1).getVersion());
        assertEquals(2L, indexDocWithId("v1", "second", 2).getVersion());
        refreshIndex(INDEX);

        VersionConflictEngineException stale = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareGet(INDEX, "v1").setVersion(1L).get()
        );
        assertTrue("expected a version conflict, got: " + stale.getMessage(), stale.getMessage().contains("version conflict"));

        // A version ahead of the stored one must conflict too, not be silently served.
        VersionConflictEngineException future = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareGet(INDEX, "v1").setVersion(99L).get()
        );
        assertTrue("expected a version conflict, got: " + future.getMessage(), future.getMessage().contains("version conflict"));
    }

    /** The same precondition on an external version type must be enforced on the segment path. */
    public void testGetWithUnsatisfiableExternalGteVersionAfterRefreshConflicts() {
        createUpdatableManualRefreshIndex();

        assertEquals(1L, indexDocWithId("v2", "first", 1).getVersion());
        assertEquals(2L, indexDocWithId("v2", "second", 2).getVersion());
        refreshIndex(INDEX);

        VersionConflictEngineException ex = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareGet(INDEX, "v2").setVersion(1L).setVersionType(VersionType.EXTERNAL_GTE).get()
        );
        assertTrue("expected a version conflict, got: " + ex.getMessage(), ex.getMessage().contains("version conflict"));
    }

    /**
     * Controls, so the enforcement above cannot be satisfied by rejecting everything: the current
     * version is served after a refresh, and a GET carrying no version at all is untouched.
     */
    public void testGetWithCurrentVersionAfterRefreshSucceeds() {
        createUpdatableManualRefreshIndex();

        assertEquals(1L, indexDocWithId("v3", "first", 1).getVersion());
        assertEquals(2L, indexDocWithId("v3", "second", 2).getVersion());
        refreshIndex(INDEX);

        GetResponse matched = client().prepareGet(INDEX, "v3").setVersion(2L).get();
        assertTrue(matched.isExists());
        assertEquals(2L, matched.getVersion());
        assertEquals(2, intValue(matched));

        GetResponse unconditional = client().prepareGet(INDEX, "v3").get();
        assertTrue(unconditional.isExists());
        assertEquals(2L, unconditional.getVersion());
        assertEquals(2, intValue(unconditional));
    }

    /**
     * The version-map tier already enforced this; kept as the before/after contrast so a regression
     * on either tier is distinguishable.
     */
    public void testGetWithStaleVersionBeforeRefreshConflicts() {
        createUpdatableManualRefreshIndex();

        assertEquals(1L, indexDocWithId("v4", "first", 1).getVersion());
        assertEquals(2L, indexDocWithId("v4", "second", 2).getVersion());
        // deliberately no refresh -- the document is still in the live version map

        VersionConflictEngineException ex = expectThrows(
            VersionConflictEngineException.class,
            () -> client().prepareGet(INDEX, "v4").setVersion(1L).setRealtime(true).get()
        );
        assertTrue("expected a version conflict, got: " + ex.getMessage(), ex.getMessage().contains("version conflict"));
    }

    public void testRealtimeGetHitsVersionMapBeforeRefresh() {
        createManualRefreshIndex();
        IndexResponse indexResponse = indexDoc("doc_1", 1);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        String docId = indexResponse.getId();

        // Non-realtime GET only sees refreshed (row) data -> not found yet, proving rows are still empty.
        GetResponse nonRealtime = client().prepareGet(INDEX, docId).setRealtime(false).get();
        assertFalse("non-realtime get must not see the unrefreshed doc", nonRealtime.isExists());

        // Realtime GET resolves from the in-memory version map (translog-backed) before any refresh.
        GetResponse realtime = client().prepareGet(INDEX, docId).setRealtime(true).get();
        assertTrue("realtime get must find the unrefreshed doc", realtime.isExists());
        assertEquals(1L, realtime.getVersion());
        assertEquals("doc_1", realtime.getSourceAsMap().get("name"));
        assertEquals(1, intValue(realtime));
    }

    public void testGetHitsRowsAfterRefresh() {
        createManualRefreshIndex();
        IndexResponse indexResponse = indexDoc("doc_2", 2);
        assertEquals(RestStatus.CREATED, indexResponse.status());
        refreshIndex(INDEX);
        String docId = indexResponse.getId();

        // After refresh the doc is materialized into parquet rows; non-realtime GET resolves via the row path.
        GetResponse resp = client().prepareGet(INDEX, docId).setRealtime(false).get();
        assertTrue("post-refresh get must find the doc via rows", resp.isExists());
        assertEquals(1L, resp.getVersion());
        assertEquals("doc_2", resp.getSourceAsMap().get("name"));
        assertEquals(2, intValue(resp));
    }

    public void testActiveIndexingWithInterleavedRefreshes() {
        createManualRefreshIndex();
        // First batch then refresh -> these live in rows.
        List<String> ids = indexDocs(INDEX, 25, 1);
        refreshIndex(INDEX);
        // Second batch, NOT refreshed -> these live only in the version map.
        ids.addAll(indexDocs(INDEX, 25, 26));

        // Refreshed id -> row path.
        GetResponse refreshed = client().prepareGet(INDEX, ids.get(9)).setRealtime(false).get();
        assertTrue("refreshed id must be found via rows", refreshed.isExists());
        assertEquals("doc_10", refreshed.getSourceAsMap().get("name"));
        assertEquals(10, intValue(refreshed));

        // Unrefreshed id -> version-map path (realtime found), absent from rows (non-realtime not found).
        GetResponse unrefreshedRealtime = client().prepareGet(INDEX, ids.get(39)).setRealtime(true).get();
        assertTrue("unrefreshed id must be found realtime via version map", unrefreshedRealtime.isExists());
        assertEquals("doc_40", unrefreshedRealtime.getSourceAsMap().get("name"));
        assertFalse("unrefreshed id must be absent from rows", client().prepareGet(INDEX, "40").setRealtime(false).get().isExists());

        // After a second refresh the previously-unrefreshed id resolves via rows too.
        refreshIndex(INDEX);
        GetResponse nowInRows = client().prepareGet(INDEX, ids.get(39)).setRealtime(false).get();
        assertTrue("after refresh id must be found via rows", nowInRows.isExists());
        assertEquals(40, intValue(nowInRows));
    }
}
