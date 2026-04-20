/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives fragment conversion for all {@link StagePlan} alternatives in a {@link QueryDAG}.
 * Strips annotations from each resolved fragment and dispatches to the backend's
 * {@link FragmentConvertor} based on the fragment's leaf type.
 *
 * <p>Currently handles two leaf types for the pure shard-scan path (PR2):
 * <ul>
 *   <li>{@link OpenSearchTableScan} — data node stage, calls {@link FragmentConvertor#convertShardScanFragment}</li>
 *   <li>{@link OpenSearchStageInputScan} — coordinator stage, calls {@link FragmentConvertor#convertCoordinatorFragment}</li>
 * </ul>
 *
 * <p>TODO: as shuffle joins/aggregates and delegation are added, this class will need to handle
 * additional leaf types (ShuffleReader, InMemory) and sink wrapping (ShuffleWriter). Rather than
 * growing this class with more instanceof checks, introduce a FragmentConversionStrategy abstraction
 * (or visitor pattern on the leaf/top nodes) so each shape encapsulates its own dispatch logic.
 * Delegation adds another dimension: per-expression sub-fragments must be extracted, converted by
 * the delegate backend, and the delegation IDs embedded in the primary fragment before conversion.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private FragmentConversionDriver() {}

    /**
     * Converts all {@link StagePlan} alternatives in the DAG, populating
     * {@link StagePlan#convertedBytes()} on each plan.
     */
    public static void convertAll(QueryDAG dag, CapabilityRegistry registry) {
        convertStage(dag.rootStage(), registry);
    }

    private static void convertStage(Stage stage, CapabilityRegistry registry) {
        for (Stage child : stage.getChildStages()) {
            convertStage(child, registry);
        }
        List<StagePlan> converted = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            AnalyticsSearchBackendPlugin backend = registry.getBackend(plan.backendId());
            FragmentConvertor convertor = backend.getFragmentConvertor();
            byte[] bytes = convert(plan.resolvedFragment(), convertor);
            converted.add(plan.withConvertedBytes(bytes));
        }
        stage.setPlanAlternatives(converted);
    }

    /**
     * Strips annotations from the resolved fragment and dispatches to the appropriate
     * {@link FragmentConvertor} method based on the leaf node type.
     */
    static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor) {
        RelNode leaf = findLeaf(resolvedFragment);
        RelNode stripped = strip(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan scan) {
            String tableName = scan.getTable().getQualifiedName().getLast();
            return convertor.convertShardScanFragment(tableName, stripped);
        }
        if (leaf instanceof OpenSearchStageInputScan) {
            return convertor.convertCoordinatorFragment(stripped);
        }
        throw new IllegalStateException(
            "Unknown leaf type [" + leaf.getClass().getSimpleName() + "]. " + "Add a FragmentConversionStrategy for this leaf type."
        );
    }

    /** Recursively strips annotations bottom-up, preserving non-OpenSearch nodes. */
    private static RelNode strip(RelNode node) {
        if (node instanceof OpenSearchStageInputScan) {
            return node;
        }
        List<RelNode> strippedChildren = new ArrayList<>(node.getInputs().size());
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            return openSearchNode.stripAnnotations(strippedChildren);
        }
        return node;
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }
}
