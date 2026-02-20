/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Builds a Calcite schema and catalog reader from OpenSearch cluster metadata.
 *
 * <p>Registers each non-system OpenSearch index as a Calcite table whose row type
 * is derived from the index mapping via {@link OpenSearchTypeMapping}.
 */
public class OpenSearchSchemaBuilder {

    private final ClusterService clusterService;

    public OpenSearchSchemaBuilder(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Build a {@link CalciteCatalogReader} reflecting the current cluster indices.
     */
    public CalciteCatalogReader buildCatalogReader() {
        JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(true);
        registerTables(rootSchema);

        Properties props = new Properties();
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
        return new CalciteCatalogReader(
            rootSchema, Collections.singletonList(""), typeFactory, config);
    }

    /**
     * Register OpenSearch indices as Calcite tables from cluster metadata.
     */
    private void registerTables(CalciteSchema rootSchema) {
        SchemaPlus schemaPlus = rootSchema.plus();
        Metadata metadata = clusterService.state().metadata();

        for (var entry : metadata.indices().entrySet()) {
            String indexName = entry.getKey();
            if (indexName.startsWith(".")) continue;
            IndexMetadata indexMetadata = entry.getValue();
            schemaPlus.add(indexName, new OpenSearchCalciteTable(indexMetadata));
        }
    }

    /**
     * Calcite table backed by an OpenSearch index, providing row type from mappings.
     */
    private static class OpenSearchCalciteTable extends AbstractTable {
        private final IndexMetadata indexMetadata;

        OpenSearchCalciteTable(IndexMetadata indexMetadata) {
            this.indexMetadata = indexMetadata;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            var builder = typeFactory.builder();

            MappingMetadata mapping = indexMetadata.mapping();
            if (mapping != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> properties = (Map<String, Object>)
                    mapping.sourceAsMap().get("properties");
                if (properties != null) {
                    addFields(builder, typeFactory, properties, "");
                }
            }

            return builder.build();
        }

        @SuppressWarnings("unchecked")
        private static void addFields(RelDataTypeFactory.Builder builder,
                                       RelDataTypeFactory typeFactory,
                                       Map<String, Object> properties,
                                       String prefix) {
            for (var prop : properties.entrySet()) {
                String fieldName = prefix.isEmpty() ? prop.getKey() : prefix + "." + prop.getKey();
                Map<String, Object> fieldProps = (Map<String, Object>) prop.getValue();
                String fieldType = (String) fieldProps.get("type");

                Map<String, Object> subProperties = (Map<String, Object>) fieldProps.get("properties");
                if (subProperties != null) {
                    addFields(builder, typeFactory, subProperties, fieldName);
                    continue;
                }

                builder.add(fieldName, typeFactory.createTypeWithNullability(
                    typeFactory.createSqlType(OpenSearchTypeMapping.mapFieldType(fieldType)), true));
            }
        }
    }
}
