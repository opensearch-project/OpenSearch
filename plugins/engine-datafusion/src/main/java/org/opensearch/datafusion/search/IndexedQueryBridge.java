/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.core.action.ActionListener;
import org.opensearch.datafusion.jni.NativeBridge;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

/**
 * Bridge between OpenSearch's Lucene index and the indexed-table Rust crate.
 *
 * Orchestrates the Lucene+DataFusion indexed query flow:
 * 1. Takes a Lucene Query and the shard's WriterFileSets (segment→parquet mapping)
 * 2. Creates a Lucene Weight from the query
 * 3. Registers it with LuceneIndexSearcher for JNI callbacks
 * 4. Gathers segment metadata (maxDoc per segment, parquet paths)
 * 5. Calls NativeBridge.executeIndexedQueryAsync() to run the indexed query
 * 6. Returns a stream pointer consumable via streamNext/streamGetSchema
 */
public class IndexedQueryBridge {

    private static final Logger logger = LogManager.getLogger(IndexedQueryBridge.class);

    /**
     * Execute an indexed query using Lucene indexes to accelerate parquet reads.
     *
     * @param luceneReader  The Lucene DirectoryReader for this shard
     * @param query         The Lucene query to execute
     * @param fileSets      The WriterFileSets mapping segments to parquet files
     * @param dataDir       The base data directory path
     * @param numPartitions Number of DataFusion partitions
     * @param bitsetMode    0 = AND (intersect), 1 = OR (union)
     * @param runtimePtr    Pointer to the DataFusion runtime
     * @param listener      ActionListener to receive the stream pointer
     */
    public static void executeIndexedQuery(
        DirectoryReader luceneReader,
        Query query,
        Collection<WriterFileSet> fileSets,
        String dataDir,
        String tableName,
        byte[] substraitBytes,
        int numPartitions,
        int bitsetMode,
        boolean isQueryPlanExplainEnabled,
        long runtimePtr,
        ActionListener<Long> listener
    ) {
        try {
            // 1. Create Lucene Weight from the query
            IndexSearcher searcher = new IndexSearcher(luceneReader);
            searcher.setQueryCache(null); // TODO: enable caching based on settings
            Query rewritten = searcher.rewrite(query);
            // TODO: enable scoring
            Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            List<LeafReaderContext> leaves = luceneReader.leaves();

            // 2. Register the Weight with LuceneIndexSearcher for JNI callbacks
            long weightPtr = LuceneIndexSearcher.registerShardWeight(searcher, weight, leaves);

            // 3. Build segment metadata arrays
            // Map Lucene segments to parquet files using writer_generation attribute.
            // Each Lucene segment has a writer_generation stored in SegmentInfo attributes.
            // Each WriterFileSet has a matching writerGeneration.
            // We iterate Lucene leaves (which define segment ordinals for scoring)
            // and find the corresponding parquet file by matching generation.

            // Build a map from writerGeneration -> WriterFileSet for O(1) lookup
            java.util.Map<Long, WriterFileSet> genToFileSet = new java.util.HashMap<>();
            for (WriterFileSet fs : fileSets) {
                genToFileSet.put(fs.getWriterGeneration(), fs);
            }

            long[] segmentMaxDocs = new long[leaves.size()];
            String[] parquetPaths = new String[leaves.size()];

            for (int i = 0; i < leaves.size(); i++) {
                segmentMaxDocs[i] = leaves.get(i).reader().maxDoc();

                // Get writer_generation from Lucene segment attributes
                long writerGen = -1;
                if (leaves.get(i).reader() instanceof org.apache.lucene.index.SegmentReader) {
                    String genAttr = ((org.apache.lucene.index.SegmentReader) leaves.get(i).reader())
                        .getSegmentInfo().info.getAttribute("writer_generation");
                    if (genAttr != null) {
                        writerGen = Long.parseLong(genAttr);
                    }
                }

                // Find the matching WriterFileSet by generation
                WriterFileSet fileSet = genToFileSet.get(writerGen);
                if (fileSet != null) {
                    String dir = fileSet.getDirectory();
                    String parquetFile = fileSet.getFiles().stream()
                        .filter(f -> f.endsWith(".parquet"))
                        .findFirst()
                        .orElse(null);
                    parquetPaths[i] = parquetFile != null ? Path.of(dir, parquetFile).toString() : "";
                } else {
                    parquetPaths[i] = "";
                }

                logger.info("[INDEXED-DEBUG] leaf[{}]: maxDoc={}, writerGen={}, parquet={}",
                    i, segmentMaxDocs[i], writerGen, parquetPaths[i]);
            }

            // 4. Call native indexed query
            // NOTE: The weight is NOT released here. The caller must call
            // LuceneIndexSearcher.releaseShardWeight(weightPtr) after the stream
            // is fully consumed, because Rust calls back into Java for scoring
            // while the stream is being read.
            logger.info("[INDEXED-DEBUG] IndexedQueryBridge: segments={}, partitions={}, bitsetMode={}, weightPtr={}, tableName={}, substraitBytes={}",
                leaves.size(), numPartitions, bitsetMode, weightPtr, tableName,
                substraitBytes != null ? substraitBytes.length + " bytes" : "null");

            NativeBridge.executeIndexedQueryAsync(
                weightPtr,
                segmentMaxDocs,
                parquetPaths,
                tableName,
                substraitBytes,
                numPartitions,
                bitsetMode,
                isQueryPlanExplainEnabled,
                runtimePtr,
                ActionListener.wrap(
                    streamPtr -> {
                        // Don't release weight here — Rust still needs it while streaming
                        listener.onResponse(streamPtr);
                    },
                    e -> {
                        // On failure, safe to release since no stream will be consumed
                        LuceneIndexSearcher.releaseShardWeight(weightPtr);
                        listener.onFailure(e);
                    }
                )
            );

        } catch (IOException e) {
            logger.error("Failed to execute indexed query", e);
            listener.onFailure(e);
        }
    }

    /**
     * Get the weight pointer for the most recently registered weight.
     * Used by the caller to release the weight after stream consumption.
     */
    public static void releaseWeight(long weightPtr) {
        LuceneIndexSearcher.releaseShardWeight(weightPtr);
    }
}
