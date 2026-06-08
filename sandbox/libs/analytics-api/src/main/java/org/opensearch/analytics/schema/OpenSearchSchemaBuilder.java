/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexNotFoundException;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds a Calcite {@link SchemaPlus} from OpenSearch {@link ClusterState} index mappings.
 *
 * <p>One Calcite table per index. Reads field types from index mapping properties.
 * Navigates: IndexMetadata -> MappingMetadata -> sourceAsMap() -> "properties" -> per-field "type".
 */
public class OpenSearchSchemaBuilder {

    private OpenSearchSchemaBuilder() {}

    public static SchemaPlus buildSchema(ClusterState clusterState) {
        return buildSchema(clusterState, new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)));
    }

    /**
     * Builds a Calcite SchemaPlus from the given ClusterState.
     *
     * <p>Tables are resolved lazily on first lookup, mirroring the sql-plugin
     * {@code OpenSearchSchema}. A requested name may be a concrete index, an alias, a comma list,
     * a wildcard, an exclusion, or date-math; it is resolved through {@code resolver} — the same
     * canonical resolution the execution-side {@code IndexResolution} uses — and the matching
     * indices' supported fields are unioned into one row type. No upfront enumeration of cluster
     * indices: construction is O(1) regardless of cluster size, and each referenced name costs
     * one {@code IndexNameExpressionResolver} call plus a single mapping union.
     *
     * <p>The lazy schema is wrapped in a NON-caching root: a caching root enumerates
     * {@code getTableNames()} and would never perform the implicit {@code getTable(name)} lookup
     * that drives lazy resolution of expressions.
     */
    public static SchemaPlus buildSchema(ClusterState clusterState, IndexNameExpressionResolver resolver) {
        Schema lazySchema = new AbstractSchema() {
            // Truly lazy table map, mirroring sql-plugin's OpenSearchSchema pattern: no upfront
            // enumeration of cluster indices. get() registers on first lookup and caches under the
            // lower-cased name. PPL's RelBuilder.scan and Calcite's case-sensitive validator both
            // reach the schema via getTable(name), which routes here directly — no entrySet /
            // keySet iteration needed for production resolution. Callers that need name
            // enumeration (Calcite's withCaseSensitive(false) parser path used in some unit tests)
            // get only resolved names back, which is fine when the lookup name is exact-case.
            private final Map<String, Table> tableMap = new HashMap<>() {
                @Override
                public Table get(Object key) {
                    String name = ((String) key).toLowerCase(java.util.Locale.ROOT);
                    if (!super.containsKey(name)) {
                        Table resolved = resolveTable(clusterState, resolver, name);
                        if (resolved != null) {
                            super.put(name, resolved);
                        }
                    }
                    return super.get(name);
                }
            };

            @Override
            protected Map<String, Table> getTableMap() {
                return tableMap;
            }
        };

        return CalciteSchema.createRootSchema(true, false, "", lazySchema).plus();
    }

    /**
     * Resolves a source expression (concrete name, alias, comma list, wildcard, exclusion, or
     * date-math) to a single table whose row type unions the supported fields of all matching
     * concrete indices, or {@code null} when nothing matches (so Calcite reports a clean "table not
     * found"). Resolution goes through {@link IndexNameExpressionResolver} so schema membership
     * matches the execution-side {@code IndexResolution}. First-wins on field-name conflict across
     * the union; the planner's scan rule validates cross-index mapping compatibility when the table
     * is referenced.
     */
    @SuppressWarnings("unchecked")
    private static Table resolveTable(ClusterState clusterState, IndexNameExpressionResolver resolver, String expression) {
        // Short-circuit literal alias / data stream names so the resolver's lenientExpandOpen
        // (which does not include hidden backings) doesn't filter out data stream backings. The
        // alias / data-stream abstraction already carries the full backing list — use it directly.
        java.util.SortedMap<String, org.opensearch.cluster.metadata.IndexAbstraction> lookup = clusterState.metadata().getIndicesLookup();
        org.opensearch.cluster.metadata.IndexAbstraction abstraction = lookup == null ? null : lookup.get(expression);
        List<IndexMetadata> backing;
        if (abstraction != null
            && (abstraction.getType() == org.opensearch.cluster.metadata.IndexAbstraction.Type.ALIAS
                || abstraction.getType() == org.opensearch.cluster.metadata.IndexAbstraction.Type.DATA_STREAM)) {
            backing = abstraction.getIndices();
        } else {
            String[] concrete;
            try {
                // Comma-split first: concreteIndexNames treats each vararg as one expression, and
                // splitting lets the resolver honor exclusions across tokens (e.g. "test*,-test1").
                // includeDataStreams=true so wildcards / comma-lists that match a data stream NAME
                // expand to its backings (the resolver normally excludes data streams from
                // wildcard expansion otherwise). Literal data stream / alias names take the
                // abstraction short-circuit above and skip the resolver entirely.
                concrete = resolver.concreteIndexNames(
                    clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    true,
                    Strings.splitStringByCommaToArray(expression)
                );
            } catch (IndexNotFoundException e) {
                return null;
            }
            backing = new java.util.ArrayList<>(concrete.length);
            for (String name : concrete) {
                IndexMetadata index = clusterState.metadata().index(name);
                if (index != null) {
                    backing.add(index);
                }
            }
        }
        LinkedHashMap<String, Object> merged = new LinkedHashMap<>();
        for (IndexMetadata index : backing) {
            MappingMetadata mapping = index.mapping();
            if (mapping == null) {
                continue;
            }
            Map<String, Object> properties = (Map<String, Object>) mapping.sourceAsMap().get("properties");
            if (properties == null) {
                continue;
            }
            properties.forEach(merged::putIfAbsent);
        }
        if (merged.isEmpty()) {
            return null;
        }
        return buildTable(merged);
    }

    /**
     * Maps an OpenSearch field type string to a Calcite SqlTypeName, or {@code null} when the type
     * has no scalar Calcite representation here (geo_point, geo_shape, nested, completion, …) or
     * is unrecognized. Callers omit the column from the schema so a query referencing it surfaces
     * a Calcite "column not found" via the validator rather than a planner-time crash.
     *
     * <p>Type mapping:
     * <ul>
     *   <li>keyword/text/match_only_text -> VARCHAR</li>
     *   <li>long/unsigned_long/scaled_float -> BIGINT</li>
     *   <li>integer -> INTEGER, short -> SMALLINT, byte -> TINYINT</li>
     *   <li>double -> DOUBLE, float/half_float -> REAL</li>
     *   <li>boolean -> BOOLEAN</li>
     *   <li>date/date_nanos -> TIMESTAMP</li>
     *   <li>ip/binary -> VARBINARY</li>
     *   <li>everything else (geo_point, geo_shape, nested, object, flat_object, completion,
     *       constant_keyword, wildcard, alias, dense_vector, sparse_vector, percolator,
     *       *_range, token_count, version, plus genuinely unknown plugin types) -> {@code null}</li>
     * </ul>
     *
     * @param opensearchType the OpenSearch field type string
     */
    public static SqlTypeName mapFieldType(String opensearchType) {
        if (opensearchType == null) {
            return null;
        }
        switch (opensearchType) {
            case "keyword":
            case "text":
            case "match_only_text":
                return SqlTypeName.VARCHAR;
            case "long":
            case "unsigned_long":
                // unsigned_long: values above 2^63 - 1 wrap into negatives because BIGINT is
                // signed and Substrait has no unsigned integer types. Smaller values are safe.
                // TODO: values above 2^63 - 1 wrap into negatives. Drop the UInt64 → Int64 narrowing
                // (see schema_coerce.rs) when we have a proper solution.
            case "scaled_float":
                return SqlTypeName.BIGINT;
            case "integer":
                return SqlTypeName.INTEGER;
            case "short":
                return SqlTypeName.SMALLINT;
            case "byte":
                return SqlTypeName.TINYINT;
            case "double":
                return SqlTypeName.DOUBLE;
            case "float":
            case "half_float":
                // half_float lands as Arrow Float16 on disk. Calcite has no fp16 type; widen to
                // REAL so the planner sees the same shape as a regular float column. The parquet
                // reader's SchemaAdapter casts Float16 → Float32 per batch.
                // TODO: every record batch goes through a Float16 → Float32 cast (see
                // schema_coerce.rs) and downstream operators see Float32. Drop the widening when
                // we have a proper solution.
                return SqlTypeName.REAL;
            case "boolean":
                return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":
                return SqlTypeName.TIMESTAMP;
            case "ip":
            case "binary":
                return SqlTypeName.VARBINARY;
            default:
                return null;
        }
    }

    /**
     * Builds the Calcite {@link RelDataType} for a leaf column given the OpenSearch field-type
     * string. For {@code ip} and {@code binary} this returns an {@link IpType} or
     * {@link BinaryType} UDT (both backed by {@link SqlTypeName#VARBINARY}); for everything
     * else it returns the {@link SqlTypeName} from {@link #mapFieldType} as a nullable basic
     * SQL type. Returns {@code null} for unrecognized / unsupported field types.
     *
     * <p>Operator dispatch on the UDTs is unaffected because both extend
     * {@link org.apache.calcite.sql.type.AbstractSqlType} with VARBINARY — the cidrmatch
     * byte-range rewrite, equality / IN / BETWEEN coercion, and Substrait conversion all see
     * the same shape they did before.
     */
    public static RelDataType buildLeafType(String opensearchType, RelDataTypeFactory typeFactory) {
        if (opensearchType == null) {
            return null;
        }
        if (IpType.NAME.equals(opensearchType)) {
            return IpType.nullable();
        }
        if (BinaryType.NAME.equals(opensearchType)) {
            return BinaryType.nullable();
        }
        SqlTypeName sqlType = mapFieldType(opensearchType);
        if (sqlType == null) {
            return null;
        }
        // date / date_nanos both map to TIMESTAMP, but their sub-second precision differs and must
        // be carried on the Calcite type (fractional-seconds digits): date → millis (3), date_nanos
        // → nanos (9). Without an explicit precision, createSqlType(TIMESTAMP) defaults to 0, which
        // downstream lowers to Timestamp(Second)→Millisecond; the parquet read of a date_nanos field
        // then produces Timestamp(Nanosecond), and the reduce-stage RowConverter rejects the mismatch.
        RelDataType base;
        if (sqlType == SqlTypeName.TIMESTAMP) {
            int precision = "date_nanos".equals(opensearchType) ? 9 : 3;
            base = typeFactory.createSqlType(sqlType, precision);
        } else {
            base = typeFactory.createSqlType(sqlType);
        }
        return typeFactory.createTypeWithNullability(base, true);
    }

    private static AbstractTable buildTable(Map<String, Object> properties) {
        return new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                RelDataTypeFactory.Builder builder = typeFactory.builder();
                addLeafFields(builder, typeFactory, properties, "");
                return builder.build();
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static void addLeafFields(
        RelDataTypeFactory.Builder builder,
        RelDataTypeFactory typeFactory,
        Map<String, Object> properties,
        String pathPrefix
    ) {
        for (Map.Entry<String, Object> fieldEntry : properties.entrySet()) {
            String fieldName = pathPrefix.isEmpty() ? fieldEntry.getKey() : pathPrefix + "." + fieldEntry.getKey();
            Map<String, Object> fieldProps = (Map<String, Object>) fieldEntry.getValue();
            String fieldType = (String) fieldProps.get("type");
            // Object types: implicit when "properties" is present without "type", or explicit "type: object".
            // Recurse into sub-properties so dotted leaf paths ("city.location.latitude") appear as flat columns.
            if (fieldType == null || "object".equals(fieldType)) {
                Map<String, Object> nested = (Map<String, Object>) fieldProps.get("properties");
                if (nested != null) {
                    addLeafFields(builder, typeFactory, nested, fieldName);
                }
                continue;
            }
            // Nested type (array-of-sub-docs) is a different beast — deferred.
            if ("nested".equals(fieldType)) {
                continue;
            }
            RelDataType columnType = buildLeafType(fieldType, typeFactory);
            if (columnType == null) {
                // Unsupported (geo_point/shape/completion/…) or unknown plugin type. Drop the
                // column; a query referencing it surfaces a Calcite "column not found" via the
                // validator rather than a planning-time IllegalArgumentException.
                continue;
            }
            builder.add(fieldName, columnType);
        }
    }
}
