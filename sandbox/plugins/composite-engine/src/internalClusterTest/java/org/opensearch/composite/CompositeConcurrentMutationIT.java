/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DocumentMissingException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mutation traffic run concurrently against the composite engine: many threads updating and deleting
 * while refreshes and merges run underneath them. Correctness is checked by get-by-id against an oracle
 * the test keeps, because physical row counts are not stable while a merge may be racing the deletes.
 *
 * <p>Every worker draws from its own {@link Random} seeded off the suite seed rather than from the test
 * framework's generator, which is single-threaded and would throw if shared across workers.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeConcurrentMutationIT extends AbstractCompositeEngineIT {

    private static final String INDEX = "concurrent_mutations";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    private static final int STORM_IDS = 200;
    private static final int STORM_THREADS = 16;
    private static final long STORM_MILLIS = 20_000;

    @Override
    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    public void setUp() throws Exception {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
        super.setUp();
    }

    @Override
    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX).get();
        } catch (Exception ignored) {
            // the index may not exist if the test failed before creating it
        }
        super.tearDown();
        System.clearProperty(MERGE_ENABLED_PROPERTY);
    }

    /** Sixteen threads mutating a small id space while refreshes run every 10 ms. */
    public void testRefreshStormDuringConcurrentMutations() throws Exception {
        createIndex("10ms");
        for (int i = 0; i < STORM_IDS; i++) {
            assertEquals(RestStatus.CREATED, indexDoc("d" + i, i).status());
        }
        refreshIndex(INDEX);

        long seed = randomLong();
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Throwable> fatal = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(STORM_THREADS + 1);
        List<Future<?>> workers = new ArrayList<>();

        workers.add(pool.submit(() -> {
            while (stop.get() == false) {
                try {
                    refreshIndex(INDEX);
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    recordIfFatal(fatal, e);
                }
            }
        }));

        for (int t = 0; t < STORM_THREADS; t++) {
            Random random = new Random(seed + t);
            workers.add(pool.submit(() -> {
                while (stop.get() == false) {
                    String id = "d" + random.nextInt(STORM_IDS);
                    try {
                        if (random.nextBoolean()) {
                            indexDoc(id, random.nextInt());
                        } else {
                            client().prepareDelete(INDEX, id).get();
                        }
                    } catch (Exception e) {
                        recordIfFatal(fatal, e);
                    }
                }
            }));
        }

        Thread.sleep(STORM_MILLIS);
        stop.set(true);
        for (Future<?> worker : workers) {
            worker.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue("seed=" + seed + ", engine-level failures during the storm: " + fatal, fatal.isEmpty());

        // The engine must still be usable: a red shard or a closed engine shows up here.
        ensureGreen(INDEX);
        assertEquals(RestStatus.CREATED, indexDoc("after_the_storm", 1).status());
        refreshIndex(INDEX);
        assertTrue("the engine must still serve reads after the storm", exists("after_the_storm"));
    }

    /**
     * Many one-document generations, each emptied by a delete while force merges run continuously. That is
     * the state {@code IndexWriter.dropDeletedSegment} reacts to, and a composite merge holding the dropped
     * segment is what takes the engine down with an NPE in {@code ReadersAndUpdates.dropMergingUpdates}.
     */
    public void testSegmentsEmptiedByDeletesWhileMergesAreInFlight() throws Exception {
        createIndex("-1");

        int generations = 400;
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < generations; i++) {
            String id = "g" + i;
            assertEquals(RestStatus.CREATED, indexDoc(id, i).status());
            ids.add(id);
            refreshIndex(INDEX);
        }

        AtomicBoolean stop = new AtomicBoolean(false);
        List<Throwable> fatal = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<?> merger = pool.submit(() -> {
            while (stop.get() == false) {
                try {
                    client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
                } catch (Exception e) {
                    recordIfFatal(fatal, e);
                }
            }
        });
        Future<?> refresher = pool.submit(() -> {
            while (stop.get() == false) {
                try {
                    refreshIndex(INDEX);
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    recordIfFatal(fatal, e);
                }
            }
        });

        // Each delete empties a whole one-row generation while the merger may be holding it.
        for (String id : ids) {
            try {
                client().prepareDelete(INDEX, id).get();
            } catch (Exception e) {
                recordIfFatal(fatal, e);
            }
        }
        stop.set(true);
        merger.get(180, TimeUnit.SECONDS);
        refresher.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue("engine-level failures while segments were emptied under merge: " + fatal, fatal.isEmpty());

        ensureGreen(INDEX);
        refreshIndex(INDEX);
        for (String id : ids) {
            assertFalse("every emptied generation's document must stay deleted: " + id, exists(id));
        }
        assertEquals(RestStatus.CREATED, indexDoc("after_the_race", 1).status());
        refreshIndex(INDEX);
        assertTrue("the engine must still serve reads", exists("after_the_race"));
    }

    /**
     * Deletes streaming in while force merges run back to back. Every deleted id must stay unresolvable
     * and every survivor must resolve once the traffic stops.
     */
    public void testForceMergesRacingDeletes() throws Exception {
        createIndex("-1");

        int total = 300;
        int perGeneration = 60;
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            String id = "d" + i;
            assertEquals(RestStatus.CREATED, indexDoc(id, i).status());
            ids.add(id);
            if ((i + 1) % perGeneration == 0) {
                refreshIndex(INDEX);
            }
        }
        refreshIndex(INDEX);

        AtomicBoolean stop = new AtomicBoolean(false);
        List<Throwable> fatal = Collections.synchronizedList(new ArrayList<>());
        ExecutorService pool = Executors.newFixedThreadPool(2);

        Future<?> merger = pool.submit(() -> {
            while (stop.get() == false) {
                try {
                    client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get();
                } catch (Exception e) {
                    recordIfFatal(fatal, e);
                }
            }
        });

        List<String> deleted = new ArrayList<>();
        for (int i = 0; i < total; i += 2) {
            String id = ids.get(i);
            assertEquals(DocWriteResponse.Result.DELETED, client().prepareDelete(INDEX, id).get().getResult());
            deleted.add(id);
        }
        stop.set(true);
        merger.get(120, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue("failures while merging against live deletes: " + fatal, fatal.isEmpty());

        refreshIndex(INDEX);
        assertEquals(0, client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get().getFailedShards());
        refreshIndex(INDEX);

        for (String id : ids) {
            assertEquals("resolution of " + id + " after merges raced deletes", deleted.contains(id) == false, exists(id));
        }
    }

    /**
     * A random operation mix, with each thread owning a disjoint slice of the id space so the expected
     * final state stays exact despite the concurrency. The seed is reported on failure.
     */
    public void testRandomOperationMixOverDisjointIdRanges() throws Exception {
        createIndex("-1");

        int threads = 8;
        int idsPerThread = 25;
        int operationsPerThread = 120;
        long seed = randomLong();

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        Map<String, Integer> expected = Collections.synchronizedMap(new LinkedHashMap<>());
        List<Throwable> fatal = Collections.synchronizedList(new ArrayList<>());
        List<Future<?>> workers = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            int threadId = t;
            Random random = new Random(seed + t);
            workers.add(pool.submit(() -> {
                Map<String, Integer> mine = new HashMap<>();
                for (int op = 0; op < operationsPerThread; op++) {
                    String id = "t" + threadId + "_d" + random.nextInt(idsPerThread);
                    try {
                        if (random.nextInt(4) == 0) {
                            client().prepareDelete(INDEX, id).get();
                            mine.remove(id);
                        } else {
                            int value = random.nextInt(100_000);
                            indexDoc(id, value);
                            mine.put(id, value);
                        }
                    } catch (Exception e) {
                        recordIfFatal(fatal, e);
                    }
                }
                expected.putAll(mine);
            }));
        }
        for (Future<?> worker : workers) {
            worker.get(120, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertTrue("seed=" + seed + ", failures during the random mix: " + fatal, fatal.isEmpty());

        refreshIndex(INDEX);
        assertEquals(0, client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).setFlush(true).get().getFailedShards());
        refreshIndex(INDEX);

        for (int t = 0; t < threads; t++) {
            for (int i = 0; i < idsPerThread; i++) {
                String id = "t" + t + "_d" + i;
                Integer value = expected.get(id);
                if (value == null) {
                    assertFalse("seed=" + seed + ", deleted id must not resolve: " + id, exists(id));
                } else {
                    assertEquals("seed=" + seed + ", value of " + id, value.intValue(), valueOf(id));
                }
            }
        }
    }

    /** Anything that is not ordinary write-path contention is a failure worth reporting. */
    private static void recordIfFatal(List<Throwable> fatal, Throwable t) {
        for (Throwable cause = t; cause != null; cause = cause.getCause()) {
            if (cause instanceof VersionConflictEngineException
                || cause instanceof DocumentMissingException
                || cause instanceof OpenSearchRejectedExecutionException) {
                return;
            }
        }
        fatal.add(t);
    }

    private void createIndex(String refreshInterval) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(IndexMetadata.INDEX_APPEND_ONLY_ENABLED_SETTING.getKey(), false)
            .put("index.refresh_interval", refreshInterval)
            .build();
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "value", "type=integer")
            .get();
        ensureGreen(INDEX);
    }

    private IndexResponse indexDoc(String id, int value) {
        return client().prepareIndex(INDEX).setId(id).setSource("name", "doc_" + id, "value", value).get();
    }

    private boolean exists(String id) {
        return client().prepareGet(INDEX, id).setRealtime(false).get().isExists();
    }

    private int valueOf(String id) {
        return ((Number) client().prepareGet(INDEX, id).setRealtime(false).get().getSourceAsMap().get("value")).intValue();
    }
}
