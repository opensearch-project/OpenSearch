/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.opensearch.Version;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Parses SQL strings into Calcite {@link RelNode} trees for planner tests.
 *
 * <p>Schema construction reuses {@link OpenSearchSchemaBuilder} — the same code path
 * production planner uses — so test SQL sees the row types the real planner receives.
 * Tests feed the returned RelNode directly into
 * {@link PlannerImpl#runAllOptimizations(RelNode, PlannerContext)}.
 *
 * <p>This fixture handles only SQL → LogicalRelNode conversion. Marking, pushdown,
 * and CBO remain the test's concern.
 */
public final class SqlPlannerTestFixture {

    private SqlPlannerTestFixture() {}

    /** Parses {@code sql} against a schema built from {@code clusterState}. */
    public static RelNode parseSql(String sql, ClusterState clusterState) {
        SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterState);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
            CalciteSchema.from(schema),
            Collections.singletonList(""),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties())
        );

        SqlValidator validator = SqlValidatorUtil.newValidator(
            SqlStdOperatorTable.instance(),
            catalogReader,
            typeFactory,
            SqlValidator.Config.DEFAULT
        );

        HepPlanner hepPlanner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(hepPlanner, new RexBuilder(typeFactory));

        SqlParser.Config parserConfig = SqlParser.config().withUnquotedCasing(Casing.UNCHANGED);
        SqlNode parsed;
        try {
            parsed = SqlParser.create(sql, parserConfig).parseQuery();
        } catch (SqlParseException e) {
            throw new AssertionError("Failed to parse SQL: " + sql, e);
        }

        SqlToRelConverter converter = new SqlToRelConverter((rowType, queryString, schemaPath, viewPath) -> {
            throw new UnsupportedOperationException("View expansion not used in tests");
        }, validator, catalogReader, cluster, StandardConvertletTable.INSTANCE, SqlToRelConverter.config());

        return converter.convertQuery(parsed, true, true).rel;
    }

    /**
     * Builds a single-index {@link ClusterState} from the given mapping {@code properties}.
     * The properties shape matches {@link OpenSearchSchemaBuilder} expectations: each
     * entry's value is a field-properties map with at minimum a {@code "type"} key.
     *
     * <p>Defaults to one shard. Use {@link #clusterStateWith(String, Map, String, int)}
     * to control the primary data format and shard count.
     */
    public static ClusterState clusterStateWith(String indexName, Map<String, Map<String, Object>> fields) {
        return clusterStateWith(indexName, fields, "parquet", 1);
    }

    /**
     * Builds a single-index {@link ClusterState} with explicit primary data format and shard count.
     * The primary format flows through to {@code FieldStorageResolver} and decides which backends
     * can scan the index's fields.
     */
    public static ClusterState clusterStateWith(
        String indexName,
        Map<String, Map<String, Object>> fields,
        String primaryDataFormat,
        int shardCount
    ) {
        try (XContentBuilder mapping = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent())) {
            mapping.startObject().field("properties", fields).endObject();
            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id)
                        .put("index.composite.primary_data_format", primaryDataFormat)
                )
                .numberOfShards(shardCount)
                .numberOfReplicas(0)
                .putMapping(mapping.toString())
                .build();
            Metadata metadata = Metadata.builder().put(indexMetadata, false).build();
            return ClusterState.builder(new ClusterName("test")).metadata(metadata).build();
        } catch (Exception e) {
            throw new AssertionError("Failed to build ClusterState for index: " + indexName, e);
        }
    }
}
