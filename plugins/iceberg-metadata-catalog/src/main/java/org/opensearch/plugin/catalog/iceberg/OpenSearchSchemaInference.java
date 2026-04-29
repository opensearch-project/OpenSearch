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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Derives an Iceberg {@link Schema} and {@link PartitionSpec} from OpenSearch
 * {@link IndexMetadata}. Best-effort mapping for MVP; schema evolution is out of scope
 * (schema is set once on first table create).
 * <p>
 * Mapping:
 * <ul>
 *   <li>{@code keyword}, {@code text}, {@code ip}, {@code object}, {@code nested}, unknown → {@code string}</li>
 *   <li>{@code long} → {@code long}</li>
 *   <li>{@code integer}, {@code short}, {@code byte} → {@code int}</li>
 *   <li>{@code double} → {@code double}</li>
 *   <li>{@code float}, {@code half_float}, {@code scaled_float} → {@code float}</li>
 *   <li>{@code boolean} → {@code boolean}</li>
 *   <li>{@code date} → {@code timestamp} (microsecond, without zone)</li>
 *   <li>{@code binary} → {@code binary}</li>
 * </ul>
 * Partition spec is always {@code identity(index_uuid), identity(shard_id)} to match the
 * per-shard publish model.
 */
public final class OpenSearchSchemaInference {

    /** System column holding the source index UUID for every row. */
    public static final String FIELD_INDEX_UUID = "index_uuid";

    /** System column holding the originating primary shard id for every row. */
    public static final String FIELD_SHARD_ID = "shard_id";

    private OpenSearchSchemaInference() {}

    /**
     * Infers a schema + partition spec for the given index metadata. The schema starts
     * with the two system columns followed by flattened document fields.
     *
     * @param indexMetadata source index metadata (reads {@code mapping().sourceAsMap()})
     * @return inferred Iceberg schema
     */
    @SuppressWarnings("unchecked")
    public static Schema inferSchema(IndexMetadata indexMetadata) {
        AtomicInteger idGen = new AtomicInteger(1);
        List<Types.NestedField> fields = new ArrayList<>();
        fields.add(Types.NestedField.required(idGen.getAndIncrement(), FIELD_INDEX_UUID, Types.StringType.get()));
        fields.add(Types.NestedField.required(idGen.getAndIncrement(), FIELD_SHARD_ID, Types.IntegerType.get()));

        MappingMetadata mapping = indexMetadata.mapping();
        if (mapping != null) {
            Map<String, Object> mappingMap = mapping.sourceAsMap();
            Object propsObj = mappingMap.get("properties");
            if (propsObj instanceof Map) {
                addFields(fields, idGen, "", (Map<String, Object>) propsObj);
            }
        }
        return new Schema(fields);
    }

    /**
     * Builds the partition spec: {@code identity(index_uuid), identity(shard_id)}.
     *
     * @param schema schema returned by {@link #inferSchema}
     * @return partition spec pinned to the system partition columns
     */
    public static PartitionSpec partitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema).identity(FIELD_INDEX_UUID).identity(FIELD_SHARD_ID).build();
    }

    @SuppressWarnings("unchecked")
    private static void addFields(List<Types.NestedField> fields, AtomicInteger idGen, String prefix, Map<String, Object> properties) {
        // Preserve insertion order for determinism across rebuilds.
        Map<String, Object> ordered = properties instanceof LinkedHashMap ? properties : new LinkedHashMap<>(properties);
        for (Map.Entry<String, Object> entry : ordered.entrySet()) {
            String name = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            Object value = entry.getValue();
            if (!(value instanceof Map)) {
                continue;
            }
            Map<String, Object> fieldDef = (Map<String, Object>) value;
            String osType = (String) fieldDef.get("type");
            Type icebergType = mapType(osType);
            // For object/nested without an explicit type, we still add a string placeholder
            // keyed by the flattened name; sub-properties (if any) are flattened below.
            fields.add(Types.NestedField.optional(idGen.getAndIncrement(), name, icebergType));
            Object nested = fieldDef.get("properties");
            if (nested instanceof Map) {
                addFields(fields, idGen, name, (Map<String, Object>) nested);
            }
        }
    }

    private static Type mapType(String osType) {
        if (osType == null) {
            return Types.StringType.get();
        }
        switch (osType) {
            case "long":
                return Types.LongType.get();
            case "integer":
            case "short":
            case "byte":
                return Types.IntegerType.get();
            case "double":
                return Types.DoubleType.get();
            case "float":
            case "half_float":
            case "scaled_float":
                return Types.FloatType.get();
            case "boolean":
                return Types.BooleanType.get();
            case "date":
                return Types.TimestampType.withoutZone();
            case "binary":
                return Types.BinaryType.get();
            case "keyword":
            case "text":
            case "ip":
            case "object":
            case "nested":
            default:
                return Types.StringType.get();
        }
    }
}
