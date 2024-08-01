/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.benchmark.search.aggregations;

import org.openjdk.jmh.annotations.*;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumericPointEncoder;
import org.opensearch.search.optimization.filterrewrite.Ranges;
import org.opensearch.search.optimization.filterrewrite.TreeTraversal;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.opensearch.search.optimization.filterrewrite.TreeTraversal.multiRangesTraverse;

@Warmup(iterations = 10)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class BKDTreeMultiRangesTraverseBenchmark {
    @State(Scope.Benchmark)
    public static class treeState {
        @Param({ "10000", "10000000" })
        int treeSize;

        @Param({ "10000", "10000000" })
        int valMax;

        @Param({ "10", "100" })
        int buckets;

        @Param({ "12345" })
        int seed;

        private Random random;

        Path tmpDir;
        Directory directory;
        IndexWriter writer;
        IndexReader reader;

        // multiRangesTraverse params
        PointValues.PointTree pointTree;
        Ranges ranges;
        BiConsumer<Integer, List<Integer>> collectRangeIDs;
        int maxNumNonZeroRanges = Integer.MAX_VALUE;

        @Setup
        public void setup() throws IOException {
            LogConfigurator.setNodeName("sample-name");
            random = new Random(seed);
            tmpDir = Files.createTempDirectory("tree-test");
            directory = FSDirectory.open(tmpDir);
            writer = new IndexWriter(directory, new IndexWriterConfig());

            for (int i = 0; i < treeSize; i++) {
                writer.addDocument(List.of(new IntField("val", random.nextInt(valMax), Field.Store.NO)));
            }

            reader = DirectoryReader.open(writer);

            // should only contain single segment
            for (LeafReaderContext lrc : reader.leaves()) {
                pointTree = lrc.reader().getPointValues("val").getPointTree();
            }

            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("val", NumberFieldMapper.NumberType.INTEGER);
            NumericPointEncoder numericPointEncoder = (NumericPointEncoder) fieldType;

            int bucketWidth = valMax/buckets;
            byte[][] lowers = new byte[buckets][];
            byte[][] uppers = new byte[buckets][];
            for (int i = 0; i < buckets; i++) {
                lowers[i] = numericPointEncoder.encodePoint(i * bucketWidth);
                uppers[i] = numericPointEncoder.encodePoint(i * bucketWidth);
            }

            ranges = new Ranges(lowers, uppers);
        }

        @TearDown
        public void tearDown() throws IOException {
            for (String indexFile : FSDirectory.listAll(tmpDir)) {
                Files.deleteIfExists(tmpDir.resolve(indexFile));
            }
            Files.deleteIfExists(tmpDir);
        }
    }

    @Benchmark
    public Map<Integer, List<Integer>> multiRangeTraverseTree(treeState state) throws Exception {
        Map<Integer, List<Integer>> mockIDCollect = new HashMap<>();

        TreeTraversal.RangeAwareIntersectVisitor treeVisitor = new TreeTraversal.DocCollectRangeAwareIntersectVisitor(state.pointTree, state.ranges, state.maxNumNonZeroRanges, (activeIndex, docID) -> {
            if (mockIDCollect.containsKey(activeIndex)) {
                mockIDCollect.get(activeIndex).add(docID);
            } else {
                mockIDCollect.put(activeIndex, List.of(docID));
            }
        });

        multiRangesTraverse(treeVisitor);
        return mockIDCollect;
    }
}
