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
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
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
 * {@link FragmentConvertor} using composable calls — backends never traverse the plan.
 *
 * <p>Dispatch logic for PR2 (pure shard-scan path):
 * <ul>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = {@link OpenSearchAggregate}(PARTIAL):
 *       {@code convertShardScanFragment} on everything below partial agg,
 *       then {@code attachPartialAggOnTop}</li>
 *   <li>Leaf = {@link OpenSearchTableScan}, top = anything else:
 *       {@code convertShardScanFragment} on the full fragment</li>
 *   <li>Leaf = {@link OpenSearchStageInputScan} (reduce stage):
 *       {@code convertFinalAggFragment} on the final agg (ExchangeReducer stripped),
 *       then {@code attachFragmentOnTop} for any operators above it</li>
 * </ul>
 *
 * <p>TODO: as shuffle joins/aggregates and delegation are added, introduce a
 * {@code FragmentConversionStrategy} abstraction so each shape encapsulates its own
 * dispatch logic rather than growing this class with more {@code instanceof} checks.
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
     * Dispatches conversion based on the fragment's leaf and top node types.
     */
    static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor) {
        RelNode leaf = findLeaf(resolvedFragment);

        if (leaf instanceof OpenSearchTableScan scan) {
            String tableName = scan.getTable().getQualifiedName().getLast();

            // Partial agg at top: convert everything below it, then attach partial agg on top.
            // strippedInputs passed to stripAnnotations for schema validity (LogicalAggregate needs its inputs).
            if (resolvedFragment instanceof OpenSearchAggregate agg && agg.getMode() == AggregateMode.PARTIAL) {
                List<RelNode> strippedInputs = agg.getInputs().stream().map(FragmentConversionDriver::strip).toList();
                byte[] innerBytes = convertor.convertShardScanFragment(tableName, strippedInputs.getFirst());
                RelNode strippedAgg = agg.stripAnnotations(strippedInputs);
                return convertor.attachPartialAggOnTop(strippedAgg, innerBytes);
            }

            return convertor.convertShardScanFragment(tableName, strip(resolvedFragment));
        }

        if (leaf instanceof OpenSearchStageInputScan) {
            return convertReduceFragment(resolvedFragment, convertor);
        }

        throw new IllegalStateException(
            "Unknown leaf type [" + leaf.getClass().getSimpleName() + "]. " + "Add a FragmentConversionStrategy for this leaf type."
        );
    }

    /**
     * Reduce stage conversion: strips ExchangeReducer, converts the final agg fragment
     * (with StageInputScan as leaf for schema), then attaches any operators above it
     * (Sort, Project, etc.) via attachFragmentOnTop.
     *
     * The node immediately above ExchangeReducer is the final agg — it goes to
     * convertFinalAggFragment together with StageInputScan. Only operators strictly
     * above the final agg use attachFragmentOnTop.
     */
    private static byte[] convertReduceFragment(RelNode node, FragmentConvertor convertor) {
        // Find the ExchangeReducer and collect operators above it
        return convertReduceNode(node, convertor, false);
    }

    private static byte[] convertReduceNode(RelNode node, FragmentConvertor convertor, boolean finalAggConverted) {
        if (node instanceof OpenSearchExchangeReducer) {
            // Strip ExchangeReducer — StageInputScan below it is the schema source
            // This should never be reached directly; handled by the parent (final agg)
            return convertor.convertFinalAggFragment(strip(node.getInputs().getFirst()));
        }
        if (node instanceof OpenSearchRelNode openSearchNode) {
            List<RelNode> strippedInputs = node.getInputs().stream().map(FragmentConversionDriver::strip).toList();
            RelNode strippedNode = openSearchNode.stripAnnotations(strippedInputs);

            if (!finalAggConverted) {
                // First OpenSearchRelNode above ExchangeReducer = final agg
                // Check if child is ExchangeReducer — if so, this is the final agg node
                boolean childIsExchangeReducer = !node.getInputs().isEmpty()
                    && node.getInputs().getFirst() instanceof OpenSearchExchangeReducer;
                if (childIsExchangeReducer) {
                    // Strip ExchangeReducer, keep StageInputScan as leaf for schema
                    RelNode stageInputScan = strip(node.getInputs().getFirst().getInputs().getFirst());
                    List<RelNode> finalAggInputs = List.of(stageInputScan);
                    RelNode finalAggFragment = openSearchNode.stripAnnotations(finalAggInputs);
                    return convertor.convertFinalAggFragment(finalAggFragment);
                }
            }

            // Operator above final agg — convert child first, then attach
            byte[] innerBytes = convertReduceNode(node.getInputs().getFirst(), convertor, false);
            return convertor.attachFragmentOnTop(strippedNode, innerBytes);
        }
        throw new IllegalStateException("Unexpected reduce stage node: " + node.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. Keeps OpenSearchStageInputScan as-is. */
    private static RelNode strip(RelNode node) {
        if (node instanceof OpenSearchStageInputScan) {
            return node; // kept for schema inference at reduce stage
        }
        if (node instanceof OpenSearchExchangeReducer) {
            return strip(node.getInputs().getFirst());
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
