/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;

import java.io.IOException;

/**
 * Converts between RelNode JSON and Calcite {@link RelNode} objects.
 *
 * <p>Uses a {@link CalciteCatalogReader} (provided externally) to resolve
 * table references during deserialization.
 */
public class CalcitePlanConverter {

    /**
     * Deserialize a RelNode JSON plan into a {@link RelNode}.
     *
     * @param jsonPlan the JSON string produced by {@code RelJsonWriter}
     * @param catalogReader catalog reader with registered tables for schema resolution
     * @return the deserialized RelNode tree
     */
    public static RelNode fromJson(String jsonPlan, CalciteCatalogReader catalogReader) {
        JavaTypeFactoryImpl typeFactory = (JavaTypeFactoryImpl) catalogReader.getTypeFactory();

        // TODO: These should be created in a separate component
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(
            JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE));
        cluster.setMetadataQuerySupplier(RelMetadataQuery::instance);

        RelJsonReader reader = new RelJsonReader(cluster, catalogReader, null);
        try {
            return reader.read(jsonPlan);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize RelNode from JSON", e);
        }
    }
}
