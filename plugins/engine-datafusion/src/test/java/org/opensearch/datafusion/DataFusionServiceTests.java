/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.search.Query;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.datafusion.search.DatafusionQuery;
import org.opensearch.datafusion.search.DatafusionSearcher;
import org.opensearch.datafusion.search.cache.CacheAccessor;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheType;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.text.TextDF;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.vectorized.execution.search.spi.DataSourceCodec;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
/**
 * Unit tests for DataFusionService
 *
 * Note: These tests require the native library to be available.
 * They are disabled by default and can be enabled by setting the system property:
 * -Dtest.native.enabled=true
 */
public class DataFusionServiceTests extends OpenSearchTestCase {

    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings mockSettings = Settings.builder().put("path.data", "/tmp/test-data").build();

        when(mockEnvironment.settings()).thenReturn(mockSettings);
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(METADATA_CACHE_ENABLED);
        clusterSettingsToAdd.add(METADATA_CACHE_SIZE_LIMIT);
        clusterSettingsToAdd.add(METADATA_CACHE_EVICTION_TYPE);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);

        service = new DataFusionService(Collections.emptyMap(), clusterSettings);
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

    // TO run update proper directory path for generation-1-optimized.parquet file in
    // this.datafusionReaderManager = new DatafusionReaderManager("TODO://FigureOutPath", formatCatalogSnapshot);
    public void testQueryPhaseExecutor() throws IOException {
        Map<String, Object[]> finalRes = new HashMap<>();
        DatafusionSearcher datafusionSearcher = null;
        try {
            DatafusionEngine engine = new DatafusionEngine(DataFormat.CSV, List.of(new FileMetadata(new TextDF(), "hits_data.parquet")), service);
            datafusionSearcher = engine.acquireSearcher("Search");

            byte[] protoContent;

            try (InputStream is = getClass().getResourceAsStream("/substrait_plan.pb")) {
                protoContent = is.readAllBytes();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            long streamPointer = datafusionSearcher.search(new DatafusionQuery(protoContent, new ArrayList<>()), service.getTokioRuntimePointer(), service.getRuntimePointer());
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(streamPointer, service.getTokioRuntimePointer() , allocator);

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

    public void testCacheOperations() {
        CacheAccessor metadataCache = service.getCacheManager().getCacheAccessor(CacheType.METADATA);

        CacheManager cacheManager= service.getCacheManager();
        String fileName = "/Users/abhital/dev/src/forkedrepo/OpenSearch/plugins/engine-datafusion/src/hits9_v1.parquet";
        // add File using CacheManager
        cacheManager.addToCache(List.of(fileName));

        // Get file using individual Cache Accessor Methods -> Prints Cache content size
        assertTrue((Boolean) metadataCache.get(fileName));

        logger.info("Memory Consumed by MetadataCache : {}",metadataCache.getMemoryConsumed());
        logger.info("Memory Consumed by CacheManager : {}",cacheManager.getTotalUsedBytes());

        logger.info("Total Configured Size Limit for MetadataCache : {}",metadataCache.getConfiguredSizeLimit());
        logger.info("Total Configured Size Limit for CacheManager : {}",cacheManager.getTotalSizeLimit());

        boolean removed = cacheManager.removeFiles(List.of(fileName));
        logger.info("Is file removed: {}. Contains File Check: {} ",removed, metadataCache.containsFile(fileName));
        logger.info("Memory Consumed by MetadataCache after removing entries: {}",metadataCache.getMemoryConsumed());
        logger.info("Memory Consumed by CacheManager after removing entries: {}",cacheManager.getTotalUsedBytes());


        // add File again to cache
        cacheManager.addToCache(List.of(fileName));
        logger.info("Entries in Metadata Cache : {}",cacheManager.getCacheAccessor(CacheType.METADATA).getEntries());

        // change cluster setting to update sizeLimit -> eventually evicts entries
        metadataCache.setSizeLimit(new ByteSizeValue(40));
        // file will be evicted as sizeLimit is decreased
        logger.info("Entries in Metadata Cache after sizeLimit exceeds: {}",cacheManager.getCacheAccessor(CacheType.METADATA).getEntries());

        // Add file again to test if cache clear works
        metadataCache.put(fileName);
        logger.info("Entries in Metadata Cache after put action with same sizeLimit: {}",cacheManager.getCacheAccessor(CacheType.METADATA).getEntries());

        metadataCache.setSizeLimit(new ByteSizeValue(500000));
        metadataCache.put(fileName);
        logger.info("Entries in Metadata Cache after put action with updatedSizeLimit: {}",cacheManager.getCacheAccessor(CacheType.METADATA).getEntries());

        metadataCache.clear();
        logger.info("Entries in Metadata Cache after Cache Clear: {}",cacheManager.getCacheAccessor(CacheType.METADATA).getEntries());

    }
}
