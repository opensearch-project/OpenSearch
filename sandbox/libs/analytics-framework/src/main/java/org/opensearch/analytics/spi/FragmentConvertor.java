/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;

/**
 * Fragment conversion API for backend plugins.
 *
 * <p>Design principle: backends never traverse the plan. Analytics-engine orchestrates
 * conversion by calling composable methods in sequence — each method converts a single
 * operator or small fragment, and the results are composed by the caller. This keeps
 * backends simple (no tree walking) and makes the conversion pipeline explicit and
 * testable at the analytics-engine level.
 *
 * <p>Composable pipeline for multi-shard aggregate with sort at coordinator:
 * <ol>
 *   <li>{@code convertShardScanFragment(tableName, Filter(Scan))} → data node inner bytes</li>
 *   <li>{@code attachPartialAggOnTop(PartialAgg, innerBytes)} → data node bytes</li>
 *   <li>{@code convertFinalAggFragment(FinalAgg(StageInputScan))} → reduce stage inner bytes</li>
 *   <li>{@code attachFragmentOnTop(Sort, innerBytes)} → reduce stage bytes</li>
 * </ol>
 *
 * <p>TODO: add {@code convertShuffleReadFragment}, {@code convertInMemoryFragment},
 * and {@code appendShuffleWriter} when shuffle joins/aggregates are implemented.
 *
 * @opensearch.internal
 */
public interface FragmentConvertor {

    /**
     * Converts a fragment whose leaf is a native physical shard scan, containing
     * everything below a partial aggregate (e.g. Filter(Scan), Scan).
     * The backend handles all operators natively — no delegation, no shuffle.
     *
     * @param tableName named table the fragment's scan references
     * @param fragment  resolved RelNode fragment (annotations stripped)
     * @return backend-specific serialized plan bytes
     */
    default byte[] convertShardScanFragment(String tableName, RelNode fragment) {
        throw new UnsupportedOperationException("convertShardScanFragment not implemented for this backend");
    }

    /**
     * Attaches a partial aggregate on top of already-converted inner bytes.
     * The backend deserializes the inner plan and wraps it with its partial
     * aggregate execution node.
     *
     * @param partialAggFragment the partial aggregate RelNode (annotations stripped, no children)
     * @param innerBytes         serialized bytes from a prior {@code convert*} call
     * @return serialized plan bytes with partial aggregate attached on top
     */
    default byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("attachPartialAggOnTop not implemented for this backend");
    }

    /**
     * Converts the final aggregate fragment at the reduce stage.
     * The leaf is a StageInputScan placeholder used for schema inference —
     * replaced at execution time with a streaming Arrow batch source
     * (e.g. StreamingTableExec in DataFusion).
     *
     * <p>TODO: revisit placement of FragmentConvertor — it references Calcite RelNode
     * and is called only by analytics-engine. Consider moving to analytics-engine and
     * removing getFragmentConvertor() from AnalyticsSearchBackendPlugin SPI.
     *
     * @param fragment resolved final aggregate RelNode (annotations stripped,
     *                 ExchangeReducer removed, StageInputScan as leaf)
     * @return backend-specific serialized plan bytes
     */
    default byte[] convertFinalAggFragment(RelNode fragment) {
        throw new UnsupportedOperationException("convertFinalAggFragment not implemented for this backend");
    }

    /**
     * Attaches a generic fragment (Sort, Project, etc.) on top of already-converted
     * inner bytes. The backend deserializes the inner plan and wraps it with the
     * given operator.
     *
     * @param fragment   the operator RelNode to attach (annotations stripped, no children)
     * @param innerBytes serialized bytes from a prior {@code convert*} or {@code attach*} call
     * @return serialized plan bytes with the fragment attached on top
     */
    default byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        throw new UnsupportedOperationException("attachFragmentOnTop not implemented for this backend");
    }
}
