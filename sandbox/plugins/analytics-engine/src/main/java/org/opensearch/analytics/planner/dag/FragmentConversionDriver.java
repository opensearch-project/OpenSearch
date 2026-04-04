/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeWriter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives fragment conversion for a {@link StagePlan}. Walks the resolved fragment
 * top-down, strips annotations via {@link OpenSearchRelNode#stripAnnotations},
 * and calls the appropriate {@link FragmentConvertor} method based on the
 * stage's leaf and top nodes.
 *
 * <p>No operator-specific logic — each node strips itself. The driver only
 * determines which convertor method to call (scan vs shuffle-read vs in-memory)
 * and whether to append a shuffle writer.
 *
 * <p>TODO: delegation-aware conversion — when annotations target a different
 * backend, extract expressions, ask delegate backend to convert, pass
 * (delegationId, delegateBytes) to primary backend's convertor.
 *
 * @opensearch.internal
 */
public class FragmentConversionDriver {

    private FragmentConversionDriver() {}

    /**
     * Strips annotations from the resolved fragment and converts it via the
     * backend's FragmentConvertor.
     *
     * @param resolvedFragment the resolved fragment from a StagePlan
     * @param convertor        the primary backend's fragment convertor
     * @return backend-specific serialized plan bytes
     */
    // TODO: push conversion dispatch into the nodes themselves (e.g., leaf node calls
    // the right convertor method, top node appends shuffle writer) to avoid instanceof
    // checks here. Similar pattern to getAnnotations()/copyResolved()/stripAnnotations().
    public static byte[] convert(RelNode resolvedFragment, FragmentConvertor convertor) {
        RelNode stripped = strip(resolvedFragment);

        String tableName = findTableName(resolvedFragment);

        // TODO: push shuffle writer detection into OpenSearchExchangeWriter via a
        // node-level method (e.g., wrapConversion(convertor, innerBytes)) instead
        // of instanceof check here.
        if (resolvedFragment instanceof OpenSearchExchangeWriter writer) {
            RelNode strippedBody = strip(writer.getInput());
            byte[] converted = convertByLeafType(strippedBody, tableName, resolvedFragment, convertor);
            return convertor.appendShuffleWriter(converted, writer.getKeys(), 0);
        }

        return convertByLeafType(stripped, tableName, resolvedFragment, convertor);
    }

    // TODO: push leaf-type dispatch into the leaf nodes themselves. Each leaf node
    // should know which FragmentConvertor method to call (e.g., OpenSearchTableScan
    // calls convertScanFragment, OpenSearchStageInputScan calls convertShuffleReadFragment
    // or convertStreamingFragment depending on parent exchange type).
    private static byte[] convertByLeafType(RelNode stripped, String tableName,
                                            RelNode original, FragmentConvertor convertor) {
        RelNode leaf = findLeaf(original);
        if (leaf instanceof OpenSearchTableScan) {
            return convertor.convertScanFragment(tableName, stripped);
        }
        if (leaf instanceof OpenSearchStageInputScan) {
            // StageInputScan under ExchangeReducer → streaming source
            // StageInputScan under ShuffleReader → shuffle source
            return convertor.convertShuffleReadFragment(tableName, stripped);
        }
        throw new IllegalStateException("Unknown leaf type: " + leaf.getClass().getSimpleName());
    }

    /** Recursively strips annotations bottom-up. */
    private static RelNode strip(RelNode node) {
        if (node instanceof OpenSearchStageInputScan) {
            return node;
        }

        List<RelNode> strippedChildren = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            strippedChildren.add(strip(input));
        }

        if (node instanceof OpenSearchRelNode openSearchNode) {
            return openSearchNode.stripAnnotations(strippedChildren);
        }
        return node;
    }

    private static String findTableName(RelNode node) {
        if (node instanceof OpenSearchTableScan scan) {
            return scan.getTable().getQualifiedName().getLast();
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) {
                return name;
            }
        }
        return null;
    }

    private static RelNode findLeaf(RelNode node) {
        if (node.getInputs().isEmpty()) {
            return node;
        }
        return findLeaf(node.getInputs().getFirst());
    }
}
