/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright OpenSearch Contributors
 */
package org.opensearch.benchmark;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class MappingDeduplicationBenchmark {
    @Param({ "1000", "10000" })
    public int indices;
    @Param({ "1000" })
    public int fields;
    @Param({ "1", "10", "0" })
    public int groups;
    private IndexMetadata[] input;
    private Metadata previous;
    private IndexMetadata changed;

    @Setup
    public void setup() throws Exception {
        input = new IndexMetadata[indices];
        int distinct = groups == 0 ? indices : groups;
        String[] mappings = new String[distinct];
        for (int i = 0; i < distinct; i++) {
            mappings[i] = mapping(fields, i);
        }
        for (int i = 0; i < indices; i++) {
            input[i] = IndexMetadata.builder("index-" + i)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .putMapping(new MappingMetadata(new CompressedXContent(mappings[i % distinct])))
                .build();
        }
        previous = fresh();
        changed = IndexMetadata.builder(previous.index("index-0"))
            .putMapping(new MappingMetadata(new CompressedXContent(mapping(fields, distinct))))
            .mappingVersion(2)
            .build();
    }

    private static String mapping(int fields, int group) {
        StringBuilder json = new StringBuilder("{\"_doc\":{\"_meta\":{\"group\":" + group + "},\"properties\":{");
        for (int i = 0; i < fields; i++) {
            if (i > 0) json.append(',');
            json.append("\"field_").append(i).append("\":{\"type\":\"keyword\"}");
        }
        return json.append("}}}").toString();
    }

    @Benchmark
    public Metadata fresh() {
        Metadata.Builder builder = Metadata.builder();
        for (IndexMetadata index : input)
            builder.put(index, false);
        return builder.build();
    }

    @Benchmark
    public Metadata unchanged() {
        return Metadata.builder(previous).build();
    }

    @Benchmark
    public Metadata oneMappingUpdate() {
        return Metadata.builder(previous).put(changed, false).build();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("indices,fields,groups,mapping_objects,compressed_bytes,estimated_mapping_heap_bytes");
        for (int count : new int[] { 100, 1000, 10000 }) {
            for (int fieldCount : new int[] { 100, 1000 }) {
                for (int groupCount : new int[] { 1, 10, 0 }) {
                    MappingDeduplicationBenchmark benchmark = new MappingDeduplicationBenchmark();
                    benchmark.indices = count;
                    benchmark.fields = fieldCount;
                    benchmark.groups = groupCount;
                    benchmark.setup();
                    Set<MappingMetadata> mappings = Collections.newSetFromMap(new IdentityHashMap<>());
                    Set<CompressedXContent> sources = Collections.newSetFromMap(new IdentityHashMap<>());
                    Set<byte[]> arrays = Collections.newSetFromMap(new IdentityHashMap<>());
                    long payload = 0;
                    long heap = 0;
                    for (IndexMetadata index : benchmark.previous.indices().values()) {
                        MappingMetadata mapping = index.mapping();
                        if (mappings.add(mapping)) heap += RamUsageEstimator.shallowSizeOf(mapping);
                        if (sources.add(mapping.source())) heap += RamUsageEstimator.shallowSizeOf(mapping.source());
                        byte[] bytes = mapping.source().compressed();
                        if (arrays.add(bytes)) {
                            payload += bytes.length;
                            heap += RamUsageEstimator.sizeOf(bytes);
                        }
                    }
                    System.out.println(count + "," + fieldCount + "," + groupCount + "," + mappings.size() + "," + payload + "," + heap);
                }
            }
        }
    }
}
