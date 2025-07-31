/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IndexReaderUtils {

    public static List<LeafReaderContext> getLeaves(int leafCount) throws Exception {
        return getLeaves(leafCount, 1);
    }

    /**
     * Utility to create leafCount number of {@link LeafReaderContext}
     * @param leafCount count of leaves to create
     * @param docsPerLeaf : Number of documents per Leaf
     * @return created leaves
     */
    public static List<LeafReaderContext> getLeaves(int leafCount, int docsPerLeaf) throws Exception {
        try (
            final Directory directory = newDirectory();
            final IndexWriter iw = new IndexWriter(
                directory,
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int i = 0; i < leafCount; ++i) {
                for (int j = 0; j < docsPerLeaf; ++j) {
                    Document document = new Document();
                    final String fieldValue = "value" + i;
                    document.add(new StringField("field1", fieldValue, Field.Store.NO));
                    iw.addDocument(document);
                }
                iw.commit();
            }
            try (DirectoryReader directoryReader = DirectoryReader.open(directory)) {
                List<LeafReaderContext> leaves = directoryReader.leaves();
                return leaves;
            }
        }
    }

    /**
     * For the Input [ 2 -> 1, 1 -> 2 ]
     * This verifies that 1 slice has 2 partitions and 2 slices has 1 partition each.
     */
    public static void verifyPartitionCountInSlices(IndexSearcher.LeafSlice[] slices, Map<Integer, Integer> leafSizesToSliceCount) {
        Map<Integer, Integer> leafCountToNumSlices = new HashMap<>();
        for (IndexSearcher.LeafSlice slice : slices) {
            int existingCount = leafCountToNumSlices.getOrDefault(slice.partitions.length, 0);
            leafCountToNumSlices.put(slice.partitions.length, existingCount + 1);
        }
        assertEquals(leafSizesToSliceCount, leafCountToNumSlices);
    }

    /**
     * For the input [ 10 -> 1, 9 -> 2 ]
     * This verifies that there is one partition with 10 docs and 2 partitions with 9 docs.
     */
    public static void verifyPartitionDocCountAcrossSlices(
        IndexSearcher.LeafSlice[] slices,
        Map<Integer, Integer> expectedDocCountToPartitionCount
    ) {
        Map<Integer, Integer> actualDocCountToPartitionCount = new HashMap<>();
        for (IndexSearcher.LeafSlice slice : slices) {
            for (IndexSearcher.LeafReaderContextPartition partition : slice.partitions) {
                int partitionDocCount = MaxTargetSliceSupplier.getPartitionDocCount(partition);
                int existingPartitionCount = actualDocCountToPartitionCount.getOrDefault(partitionDocCount, 0);
                actualDocCountToPartitionCount.put(partitionDocCount, existingPartitionCount + 1);
            }
        }
        assertEquals(expectedDocCountToPartitionCount, actualDocCountToPartitionCount);
    }

    public static void verifyUniqueSegmentPartitionsPerSlices(IndexSearcher.LeafSlice[] slices) {
        for (IndexSearcher.LeafSlice slice : slices) {
            Set<Integer> partitionSeen = new HashSet<>();
            for (IndexSearcher.LeafReaderContextPartition partition : slice.partitions) {
                assertFalse(partitionSeen.contains(partition.ctx.ord));
                partitionSeen.add(partition.ctx.ord);
            }
        }
    }

}
