/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.opensearch.analytics.planner.CapabilityResolutionUtils;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.List;

/**
 * Converts {@link TableScan} → {@link OpenSearchTableScan}.
 * Resolves backend from index data format settings and populates
 * per-column {@link FieldStorageInfo} from IndexMetadata mappings.
 *
 * @opensearch.internal
 */
public class OpenSearchTableScanRule extends RelOptRule {

    private final PlannerContext context;

    public OpenSearchTableScanRule(PlannerContext context) {
        super(operand(TableScan.class, none()), "OpenSearchTableScanRule");
        this.context = context;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        if (scan instanceof OpenSearchTableScan) {
            return;
        }

        // TODO: table name can be an index pattern — needs IndexNameExpressionResolver
        String tableName = scan.getTable().getQualifiedName().getLast();

        IndexMetadata indexMetadata = context.getClusterState().metadata().index(tableName);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("Index [" + tableName + "] not found in cluster state");
        }

        String primaryFormat = indexMetadata.getSettings().get("index.composite.primary_data_format", "lucene");
        List<String> viableBackends = CapabilityResolutionUtils.computeViableBackends(
            context.getBackends(), OperatorCapability.SCAN, primaryFormat);
        if (viableBackends.isEmpty()) {
            throw new IllegalStateException("No backend supports format [" + primaryFormat
                + "] with SCAN capability for index [" + indexMetadata.getIndex().getName() + "]");
        }

        List<String> fieldNames = scan.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .toList();
        List<FieldStorageInfo> fieldStorage = FieldStorageResolver.resolve(indexMetadata, fieldNames);

        call.transformTo(OpenSearchTableScan.create(
            scan.getCluster(), scan.getTable(), viableBackends.getFirst(), viableBackends, fieldStorage,
            indexMetadata.getNumberOfShards(), context.getDistributionTraitDef()
        ));
    }
}
