/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import com.parquet.parquetdataformat.ParquetDataFormatPlugin;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.core.DataFusionRuntimeEnv;
import org.opensearch.datafusion.search.DatafusionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineSearcherSupplier;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.*;
import org.opensearch.tasks.Task;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.vectorized.execution.search.DataFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.common.unit.TimeValue.timeValueMinutes;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTests extends OpenSearchSingleNodeTestCase {

    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Mock
    private ClusterService clusterService;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        service = new DataFusionService(Map.of(), clusterService, "/tmp");
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);
        clusterSettingsToAdd.add(DataFusionRuntimeEnv.DATAFUSION_MEMORY_POOL_CONFIGURATION);
        clusterSettingsToAdd.add(DataFusionRuntimeEnv.DATAFUSION_SPILL_MEMORY_LIMIT_CONFIGURATION);


        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        service = new DataFusionService(Collections.emptyMap(), clusterService, "/tmp");
        //service = new DataFusionService(Map.of());
        service.doStart();
    }

    public void testGetVersion() {
        String version = service.getVersion();
        assertNotNull(version);
        assertTrue(version.contains("datafusion_version"));
        assertTrue(version.contains("substrait_version"));
    }

//    public void testCreateAndCloseContext() {
//        // Create context
//        SessionContext defaultContext = service.getDefaultContext();
//        assertNotNull(defaultContext);
//        assertTrue(defaultContext.getContext() > 0);
//
//        // Verify context exists
//        SessionContext context = service.getContext(defaultContext.getContext());
//        assertNotNull(context);
//        assertEquals(defaultContext.getContext(), context.getContext());
//
//        // Close context
//        boolean closed = service.closeContext(defaultContext.getContext());
//        assertTrue(closed);
//
//        // Verify context is gone
//        assertNull(service.getContext(defaultContext.getContext()));
//    }

    public void testQueryPhaseExecutor() throws IOException {
        Map<String, Object[]> finalRes = new HashMap<>();
        DatafusionSearcher datafusionSearcher = null;
        try {
            URL resourceUrl = getClass().getClassLoader().getResource("data/");
            Index index = new Index("index-7", "index-7");
            final Path path = Path.of(resourceUrl.toURI()).resolve("index-7").resolve("0");
            ShardPath shardPath = new ShardPath(false, path, path, new ShardId(index, 0));
            DatafusionEngine engine = new DatafusionEngine(DataFormat.CSV, List.of(new FileMetadata(DataFormat.CSV.toString(), "generation-1.parquet")), service, shardPath);
            datafusionSearcher = engine.acquireSearcher("search");

            byte[] protoContent;
            try (InputStream is = getClass().getResourceAsStream("/substrait_plan.pb")) {
                protoContent = is.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            long streamPointer = datafusionSearcher.search(new DatafusionQuery(index.getName(), protoContent, new ArrayList<>(), false), service.getRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, service.getRuntimePointer(), allocator);

            // We can have some collectors passed like this which can collect the results and convert to InternalAggregation
            // Is the possible? need to check

            SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                @Override
                public void collect(RecordBatchStream value) {
                    VectorSchemaRoot root = value.getVectorSchemaRoot();
                    for (Field field : root.getSchema().getFields()) {
                        String filedName = field.getName();
                        FieldVector fieldVector = root.getVector(filedName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        for (int i = 0; i < fieldVector.getValueCount(); i++) {
                            fieldValues[i] = fieldVector.getObject(i);
                        }
                        finalRes.put(filedName, fieldValues);
                    }
                }
            };

            while (stream.loadNextBatch().join()) {
                collector.collect(stream);
            }

            logger.info("Final Results:");
            for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
                logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
            }

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
        }
        finally {
            if(datafusionSearcher != null) {
                datafusionSearcher.close();
            }
        }
    }

    public void testQueryThenFetchExecutor() throws IOException, URISyntaxException {
        DatafusionSearcher datafusionSearcher = null;
        try {
            URL resourceUrl = getClass().getClassLoader().getResource("data/");
            Index index = new Index("index-7", "index-7");
            final Path path = Path.of(resourceUrl.toURI()).resolve("index-7").resolve("0");
            ShardPath shardPath = new ShardPath(false, path, path, new ShardId(index, 0));
            DatafusionEngine engine = new DatafusionEngine(DataFormat.CSV, List.of(new FileMetadata(DataFormat.CSV.toString(), "generation-1.parquet"), new FileMetadata(DataFormat.CSV.toString(), "generation-2.parquet")), service, shardPath);
            datafusionSearcher = engine.acquireSearcher("Search");

            byte[] protoContent;
            try (InputStream is = getClass().getResourceAsStream("/substrait_plan.pb")) {
                protoContent = is.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            DatafusionQuery query = new DatafusionQuery(index.getName(), protoContent, new ArrayList<>(), false);
            long streamPointer = datafusionSearcher.search(query, service.getRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, service.getRuntimePointer(), allocator);

            ArrayList<Long> row_ids_res = new ArrayList<>();

            while (stream.loadNextBatch().join()) {
                VectorSchemaRoot root = stream.getVectorSchemaRoot();
                for (Field field : root.getSchema().getFields()) {
                    String fieldName = field.getName();
                    if (fieldName.equals("___row_id")) {
                        BigIntVector fieldVector = (BigIntVector) root.getVector(fieldName);
                        for(int i=0; i<fieldVector.getValueCount(); i++) {
                            row_ids_res.add(fieldVector.get(i));
                        }
                    }
                }
            }

            logger.info("Final row_ids count: {}", row_ids_res);

            List<String> projections = List.of("message");
            query.setSource(projections, List.of());
            query.setFetchPhaseContext(row_ids_res);
            long fetchPhaseStreamPointer = datafusionSearcher.search(query, service.getRuntimePointer());

            RecordBatchStream fetchPhaseStream = new RecordBatchStream(fetchPhaseStreamPointer, service.getRuntimePointer(), allocator);
            int total_fetch_results = 0;
            ArrayList<Long> fetch_row_ids_res = new ArrayList<>();

            while(fetchPhaseStream.loadNextBatch().join()) {
                VectorSchemaRoot root = fetchPhaseStream.getVectorSchemaRoot();
                assertEquals(projections.size(), root.getSchema().getFields().size());
                for (Field field : root.getSchema().getFields()) {
                    assertTrue("Field was not passed in projections list", projections.contains(field.getName()));
                    if(field.getName().equals("___row_id")) {
                        IntVector fieldVector = (IntVector) root.getVector(field.getName());
                        for(int i=0; i<root.getSchema().getFields().size(); i++) {
                            fetch_row_ids_res.add((long) fieldVector.get(i));
                        }
                    } else if(field.getName().equals("target_ip")) {
                        ViewVarBinaryVector fieldVector = (ViewVarBinaryVector) root.getVector(field.getName());

                    }
                }
                total_fetch_results += root.getRowCount();
            }

            assertEquals(row_ids_res.size(), total_fetch_results);
        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw exception;
        } finally {
            if(datafusionSearcher != null) {
                datafusionSearcher.close();
            }
        }
    }

    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(ParquetDataFormatPlugin.class);
    }

    public void testQueryThenFetchE2ETest() throws IOException, URISyntaxException, InterruptedException, ExecutionException {
        URL resourceUrl = getClass().getClassLoader().getResource("data/");
        Index index = new Index("index-7", "index-7");
        final Path path = Path.of(resourceUrl.toURI()).resolve("index-7").resolve("0");
        ShardPath shardPath = new ShardPath(false, path, path, new ShardId(index, 0));
        DatafusionEngine engine = new DatafusionEngine(DataFormat.CSV, List.of(new FileMetadata(DataFormat.CSV.toString(), "generation-1.parquet"), new FileMetadata(DataFormat.CSV.toString(), "generation-2.parquet")), service, shardPath);

        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true).source(new SearchSourceBuilder().size(9).fetchSource(List.of("message").toArray(String[]::new), null));
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            OriginalIndices.NONE,
            searchRequest,
            new ShardId(index, 0),
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            -1,
            null,
            null
        );

        IndexService indexService = createIndex("index-7", Settings.EMPTY, jsonBuilder().startObject()
            .startObject("properties")
            .startObject("___row_id")
            .field("type", "long")
            .endObject()
            .startObject("message")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject()
        );
        ThreadPool threadPool = new TestThreadPool(this.getClass().getName());
        IndexShard indexShard = createIndexShard(shardPath.getShardId(), true);
        when(indexShard.getThreadPool()).thenReturn(threadPool);
        SearchOperationListener searchOperationListener = new SearchOperationListener() {
        };
        when(indexShard.getSearchOperationListener()).thenReturn(searchOperationListener);

        EngineSearcherSupplier<?> reader = indexShard.acquireSearcherSupplier();
        ReaderContext readerContext = createAndPutReaderContext(shardSearchRequest, indexService, indexShard, reader);
        SearchShardTarget searchShardTarget = new SearchShardTarget("node_1", new ShardId("index-7", "index-7", 0), null, OriginalIndices.NONE);
        SearchShardTask searchShardTask = new SearchShardTask(0, "n/a", "n/a", "test", null, Collections.singletonMap(Task.X_OPAQUE_ID, "my_id"));
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, shardSearchRequest, searchShardTarget, searchShardTask, engine, null, null);

        byte[] protoContent;
        try (InputStream is = getClass().getResourceAsStream("/substrait_plan_test.pb")) {
            protoContent = is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DatafusionQuery query = new DatafusionQuery(index.getName(), protoContent, new ArrayList<>(), false);
        List<String> projections = List.of("message");
        query.setSource(projections, List.of());

        datafusionContext.datafusionQuery(query);

        engine.executeQueryPhase(datafusionContext);
        int totalHits = Math.toIntExact(datafusionContext.queryResult().getTotalHits().value());
        int[] docIdsToLoad = new int[totalHits];
        for (int i=0; i<totalHits; i++) {
            docIdsToLoad[i] = datafusionContext.queryResult().topDocs().topDocs.scoreDocs[i].doc;
        }
        datafusionContext.docIdsToLoad(docIdsToLoad, 0, totalHits);
        engine.executeFetchPhase(datafusionContext);

        assertTrue(datafusionContext.fetchResult().hits().getHits().length > 0);
        assertEquals(datafusionContext.docIdsToLoad().length, datafusionContext.fetchResult().hits().getTotalHits().value());
    }

    final AtomicLong idGenerator = new AtomicLong();


    final ReaderContext createAndPutReaderContext(
        ShardSearchRequest request,
        IndexService indexService,
        IndexShard shard,
        EngineSearcherSupplier<?> reader
    ) {
        assert request.readerId() == null;
        assert request.keepAlive() == null;
        ReaderContext readerContext = null;
        Releasable decreaseScrollContexts = null;
        try {

            final long keepAlive = request.keepAlive() != null ? request.keepAlive().getMillis() : request.readerId() == null ? timeValueMinutes(5).getMillis() : -1;

            final ShardSearchContextId id = new ShardSearchContextId(UUIDs.randomBase64UUID(), idGenerator.incrementAndGet());

            readerContext = new ReaderContext(id, indexService, shard, reader, keepAlive, request.keepAlive() == null);
            reader = null;
            final ReaderContext finalReaderContext = readerContext;
            final SearchOperationListener searchOperationListener = shard.getSearchOperationListener();
            searchOperationListener.onNewReaderContext(finalReaderContext);
            readerContext.addOnClose(() -> {
                try {
                    if (finalReaderContext.scrollContext() != null) {
                        searchOperationListener.onFreeScrollContext(finalReaderContext);
                    }
                } finally {
                    searchOperationListener.onFreeReaderContext(finalReaderContext);
                }
            });
            readerContext = null;
            return finalReaderContext;
        } finally {
            Releasables.close(reader, readerContext, decreaseScrollContexts);
        }
    }

    static IndexShard createIndexShard(ShardId shardId, boolean remoteStoreEnabled) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, String.valueOf(remoteStoreEnabled))
            .build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test_index", settings);
        Store store = mock(Store.class);
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.indexSettings()).thenReturn(indexSettings);
        when(indexShard.shardId()).thenReturn(shardId);
        when(indexShard.store()).thenReturn(store);
        return indexShard;
    }
}
