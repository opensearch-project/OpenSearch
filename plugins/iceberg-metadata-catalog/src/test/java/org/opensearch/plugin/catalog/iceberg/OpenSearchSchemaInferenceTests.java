/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

public class OpenSearchSchemaInferenceTests extends OpenSearchTestCase {

    public void testInferSchemaWithoutMappingsHasSystemColumns() {
        IndexMetadata metadata = minimalIndex("no-mappings");
        Schema schema = OpenSearchSchemaInference.inferSchema(metadata);

        assertEquals(2, schema.columns().size());
        assertEquals(OpenSearchSchemaInference.FIELD_INDEX_UUID, schema.columns().get(0).name());
        assertEquals(Types.StringType.get(), schema.columns().get(0).type());
        assertEquals(OpenSearchSchemaInference.FIELD_SHARD_ID, schema.columns().get(1).name());
        assertEquals(Types.IntegerType.get(), schema.columns().get(1).type());
    }

    public void testPartitionSpecIsIdentityIndexUuidShardId() {
        IndexMetadata metadata = minimalIndex("parts");
        Schema schema = OpenSearchSchemaInference.inferSchema(metadata);
        PartitionSpec spec = OpenSearchSchemaInference.partitionSpec(schema);
        assertEquals(2, spec.fields().size());
        assertEquals(OpenSearchSchemaInference.FIELD_INDEX_UUID, spec.fields().get(0).name());
        assertEquals(OpenSearchSchemaInference.FIELD_SHARD_ID, spec.fields().get(1).name());
    }

    public void testPrimitiveTypeMappings() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"kw\":{\"type\":\"keyword\"},"
            + "\"tx\":{\"type\":\"text\"},"
            + "\"ip\":{\"type\":\"ip\"},"
            + "\"lg\":{\"type\":\"long\"},"
            + "\"in\":{\"type\":\"integer\"},"
            + "\"sh\":{\"type\":\"short\"},"
            + "\"by\":{\"type\":\"byte\"},"
            + "\"db\":{\"type\":\"double\"},"
            + "\"fl\":{\"type\":\"float\"},"
            + "\"hf\":{\"type\":\"half_float\"},"
            + "\"sf\":{\"type\":\"scaled_float\"},"
            + "\"bo\":{\"type\":\"boolean\"},"
            + "\"dt\":{\"type\":\"date\"},"
            + "\"bn\":{\"type\":\"binary\"},"
            + "\"un\":{\"type\":\"unknown_type\"}"
            + "}}";
        IndexMetadata metadata = IndexMetadata.builder("types").settings(minimalIndexSettings()).putMapping(mapping).build();
        Schema schema = OpenSearchSchemaInference.inferSchema(metadata);

        assertEquals(Types.StringType.get(), schema.findField("kw").type());
        assertEquals(Types.StringType.get(), schema.findField("tx").type());
        assertEquals(Types.StringType.get(), schema.findField("ip").type());
        assertEquals(Types.LongType.get(), schema.findField("lg").type());
        assertEquals(Types.IntegerType.get(), schema.findField("in").type());
        assertEquals(Types.IntegerType.get(), schema.findField("sh").type());
        assertEquals(Types.IntegerType.get(), schema.findField("by").type());
        assertEquals(Types.DoubleType.get(), schema.findField("db").type());
        assertEquals(Types.FloatType.get(), schema.findField("fl").type());
        assertEquals(Types.FloatType.get(), schema.findField("hf").type());
        assertEquals(Types.FloatType.get(), schema.findField("sf").type());
        assertEquals(Types.BooleanType.get(), schema.findField("bo").type());
        assertEquals(Types.TimestampType.withoutZone(), schema.findField("dt").type());
        assertEquals(Types.BinaryType.get(), schema.findField("bn").type());
        assertEquals(Types.StringType.get(), schema.findField("un").type());
    }

    public void testNestedFieldFlattened() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"outer\":{\"type\":\"object\",\"properties\":{"
            + "\"inner\":{\"type\":\"long\"}"
            + "}}"
            + "}}";
        IndexMetadata metadata = IndexMetadata.builder("nested").settings(minimalIndexSettings()).putMapping(mapping).build();
        Schema schema = OpenSearchSchemaInference.inferSchema(metadata);
        assertEquals(Types.StringType.get(), schema.findField("outer").type());
        assertEquals(Types.LongType.get(), schema.findField("outer.inner").type());
    }

    // ---- helpers ----

    private static Settings minimalIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
    }

    private static IndexMetadata minimalIndex(String name) {
        return IndexMetadata.builder(name).settings(minimalIndexSettings()).build();
    }
}
