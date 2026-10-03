/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.join;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.canmatch.CanMatchFilter;
import org.opensearch.analytics.exec.canmatch.LongSet;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.planner.rel.OpenSearchBroadcastScan;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Derives can-match runtime filters from an already-captured broadcast build side and attaches them to
 * the stage that probes it.
 *
 * <p>This is the broadcast family's "free" production path (design §5.2). {@code UnifiedDispatch}'s
 * capture phase has, by this point, run every broadcast build to completion and holds its whole output
 * as Arrow IPC bytes on the coordinator — before the probe stage is dispatched. So the build-side key
 * set costs one pass over bytes already in hand: no extra scan, no new stage, and no question about
 * whether the filter arrives in time.
 *
 * <p>What this buys over the native DataFusion dynamic filter, which already fires on these shapes: a
 * shard that holds no build key is never contacted at all. The native mechanism cannot reach that far —
 * it can only prune row groups inside a scan that has already been dispatched.
 *
 * <p>Everything here fails open. Any obstacle — a non-integral key, a key set over budget, an
 * unreadable batch, a probe side this join type forbids filtering — produces no filter, and a query
 * with no filter is simply the query as it runs today.
 *
 * @opensearch.internal
 */
public final class BroadcastRuntimeFilters {

    private static final Logger LOGGER = LogManager.getLogger(BroadcastRuntimeFilters.class);

    private BroadcastRuntimeFilters() {}

    /**
     * Walks the DAG and attaches a {@link LongSet} can-match filter to every shard stage that probes a
     * captured broadcast build on an integral equi-key.
     *
     * <p>Must run at the same point as {@code UnifiedDispatch.injectBroadcastsInPlace} and for the same
     * reason: the shuffle promotion replaces every stage's plan alternatives, so anything attached
     * earlier is wiped. Attaching is in-place and additive, so it composes with the broadcast injection
     * and the shuffle enrichment that touch the same stage objects.
     *
     * @param stage      DAG node to walk (call with the root)
     * @param captured   build-stage id → captured Arrow IPC bytes
     * @param allocator  query-scoped allocator for reading those bytes
     * @param maxValues  largest key set to ship; above this no filter is produced for that join
     * @return how many stages gained at least one filter — every decline here is silent by design, so
     *     this is the only way to tell "no join was eligible" from "the mechanism did not run"
     */
    public static int attach(Stage stage, Map<Integer, byte[]> captured, BufferAllocator allocator, int maxValues) {
        if (stage == null) {
            return 0;
        }
        int attached = maxValues > 0 ? attachToStage(stage, captured, allocator, maxValues) : 0;
        for (Stage child : stage.getChildStages()) {
            attached += attach(child, captured, allocator, maxValues);
        }
        return attached;
    }

    private static int attachToStage(Stage stage, Map<Integer, byte[]> captured, BufferAllocator allocator, int maxValues) {
        RelNode fragment = stage.getFragment();
        if (fragment == null) {
            return 0;
        }
        // Only a shard fragment reaches the can-match phase; on any other stage type the filter would be
        // carried and never consulted.
        if (stage.getExecutionType() != StageExecutionType.SHARD_FRAGMENT) {
            return 0;
        }

        List<CanMatchFilter> filters = new ArrayList<>();
        for (OpenSearchJoin join : RelNodeUtils.findNodes(fragment, OpenSearchJoin.class)) {
            filters.addAll(filtersForJoin(join, captured, allocator, maxValues));
        }
        if (filters.isEmpty()) {
            return 0;
        }
        stage.addRuntimeCanMatchFilters(filters);
        LOGGER.debug("[runtime-filter] stage {} gained {} can-match filter(s): {}", stage.getStageId(), filters.size(), filters);
        return 1;
    }

    /** Filters derivable from one join whose build side is a captured broadcast. */
    private static List<CanMatchFilter> filtersForJoin(
        OpenSearchJoin join,
        Map<Integer, byte[]> captured,
        BufferAllocator allocator,
        int maxValues
    ) {
        int buildIdx = -1;
        byte[] ipc = null;
        for (int i = 0; i < join.getInputs().size(); i++) {
            if (RelNodeUtils.unwrapHep(join.getInput(i)) instanceof OpenSearchBroadcastScan scan) {
                byte[] bytes = captured.get(scan.getBuildStageId());
                if (bytes != null) {
                    buildIdx = i;
                    ipc = bytes;
                    break;
                }
            }
        }
        if (buildIdx < 0) {
            // The likeliest reason this family's counter stays at zero: no captured broadcast among the
            // join's DIRECT inputs. The injection path searches the whole fragment, so a shape that wraps
            // the scan injects the memtable correctly and still yields no filter here.
            LOGGER.debug(
                "[runtime-filter] join has no captured broadcast among its {} direct input(s); no filter",
                join.getInputs().size()
            );
            return List.of();
        }
        int probeIdx = buildIdx == 0 ? 1 : 0;
        if (!RuntimeFilterEligibility.canFilterProbeSide(join.getJoinType(), probeIdx)) {
            LOGGER.debug(
                "[runtime-filter] join type {} preserves input {}; a probe-side filter would drop rows",
                join.getJoinType(),
                probeIdx
            );
            return List.of();
        }

        JoinInfo info = join.analyzeCondition();
        if (info.leftKeys.isEmpty()) {
            return List.of();
        }
        List<Integer> buildKeys = buildIdx == 0 ? info.leftKeys : info.rightKeys;
        List<Integer> probeKeys = buildIdx == 0 ? info.rightKeys : info.leftKeys;

        List<String> probeFieldNames = join.getInput(probeIdx).getRowType().getFieldNames();
        List<CanMatchFilter> filters = new ArrayList<>();
        for (int k = 0; k < buildKeys.size(); k++) {
            int probeOrdinal = probeKeys.get(k);
            if (probeOrdinal < 0 || probeOrdinal >= probeFieldNames.size()) {
                continue;
            }
            // The field name is used verbatim as the parquet column name, as CanMatchFilterExtractor does
            // for WHERE-derived filters. A renaming Project yields a name the data node cannot resolve, and
            // the native side answers UNKNOWN — no pruning, no incorrectness.
            String probeColumn = probeFieldNames.get(probeOrdinal);
            // A project that KEEPS the name while changing the value is the dangerous case: the name still
            // resolves, to the raw parquet column, while these values are the join's view of the key.
            // Pruning on that comparison would drop rows that belong in the result.
            if (!RuntimeFilterEligibility.keyPassesThroughProjectsUnchanged(join.getInput(probeIdx), probeColumn)) {
                LOGGER.debug("[runtime-filter] probe key '{}' is redefined by a project; no can-match filter", probeColumn);
                continue;
            }
            long[] values = distinctIntegralValues(ipc, buildKeys.get(k), allocator, maxValues);
            if (values == null || values.length == 0) {
                continue;
            }
            filters.add(new LongSet(probeColumn, values));
        }
        return filters;
    }

    /**
     * Distinct values of column {@code ordinal} across the captured batches, or {@code null} when no
     * value set can be produced: a non-integral column, more than {@code maxValues} distinct values, an
     * empty build side, or an unreadable stream.
     *
     * <p>Nulls are skipped. A null build key matches nothing under equi-join semantics, so it contributes
     * no candidate; and it cannot widen the filter either, because a probe row whose key is null likewise
     * matches nothing.
     *
     * <p>Aborts as soon as the distinct count exceeds the cap rather than collecting the whole set and
     * checking afterwards — a wide build side is exactly the case where collecting would cost the most.
     */
    // Package-private for the same reason as canFilterProbeSide: the null, over-cap, empty and
    // wrong-type paths all have to yield "no filter", and asserting that directly is cheaper and clearer
    // than driving it through a constructed plan.
    static long[] distinctIntegralValues(byte[] ipc, int ordinal, BufferAllocator allocator, int maxValues) {
        Set<Long> distinct = new LinkedHashSet<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (ordinal < 0 || ordinal >= root.getFieldVectors().size()) {
                return null;
            }
            while (reader.loadNextBatch()) {
                FieldVector vector = root.getVector(ordinal);
                if (!isIntegral(vector)) {
                    LOGGER.debug("[runtime-filter] build key column {} is {}; not an integral type", ordinal, vector.getMinorType());
                    return null;
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    if (vector.isNull(row)) {
                        continue;
                    }
                    distinct.add(integralAt(vector, row));
                    if (distinct.size() > maxValues) {
                        LOGGER.debug("[runtime-filter] build key column {} exceeds {} distinct values; no filter", ordinal, maxValues);
                        return null;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.debug("[runtime-filter] could not read captured build side (no filter)", e);
            return null;
        }
        if (distinct.isEmpty()) {
            // An empty build side means an inner join yields nothing at all. That is a whole-query short
            // circuit and is deliberately NOT expressed as a filter here — see LongSet's class doc.
            return null;
        }
        long[] values = new long[distinct.size()];
        int i = 0;
        for (long value : distinct) {
            values[i++] = value;
        }
        return values;
    }

    /**
     * Integral Arrow types whose parquet physical representation is `INT32` or `INT64`, which is what the
     * data node compares row-group statistics in. Deliberately excludes floating point and decimal: their
     * parquet encodings do not compare as a plain {@code long}, and a wrong comparison here would prune a
     * shard that holds matching rows.
     */
    private static boolean isIntegral(FieldVector vector) {
        return vector instanceof TinyIntVector
            || vector instanceof SmallIntVector
            || vector instanceof IntVector
            || vector instanceof BigIntVector
            || vector instanceof DateDayVector
            || vector instanceof DateMilliVector;
    }

    private static long integralAt(FieldVector vector, int row) {
        if (vector instanceof TinyIntVector v) {
            return v.get(row);
        }
        if (vector instanceof SmallIntVector v) {
            return v.get(row);
        }
        if (vector instanceof IntVector v) {
            return v.get(row);
        }
        if (vector instanceof BigIntVector v) {
            return v.get(row);
        }
        if (vector instanceof DateDayVector v) {
            return v.get(row);
        }
        if (vector instanceof DateMilliVector v) {
            return v.get(row);
        }
        throw new IllegalStateException("not an integral vector: " + vector.getMinorType());
    }
}
