/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.datafusion.core.SessionContext;
import org.opensearch.index.engine.SearchExecutionEngine;
import org.opensearch.search.aggregations.SearchResultsCollector;

import java.util.HashMap;
import java.util.Map;

/**
 * DataFusion search execution engine implementation that executes Substrait query plans
 * using the DataFusion query engine for OpenSearch.
 */
public class DatafusionEngine implements SearchExecutionEngine {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);
    private final DataFusionService dataFusionService;

    /**
     * Constructs a new DatafusionEngine with the specified DataFusion service.
     *
     * @param dataFusionService the DataFusion service used for query execution
     */
    public DatafusionEngine(DataFusionService dataFusionService) {
        this.dataFusionService = dataFusionService;
    }

    @Override
    public Map<String, Object[]> execute(byte[] queryPlanIR) {
        Map<String, Object[]> finalRes = new HashMap<>();
        try {
            SessionContext defaultSessionContext = dataFusionService.getDefaultContext();
            long streamPointer = dataFusionService.executeSubstraitQueryStream(queryPlanIR);
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            RecordBatchStream stream = new RecordBatchStream(defaultSessionContext, streamPointer, allocator);

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
        return finalRes;
    }
}
