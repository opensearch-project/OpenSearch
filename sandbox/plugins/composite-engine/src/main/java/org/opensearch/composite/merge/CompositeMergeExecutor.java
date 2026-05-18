/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.stats.CompositeShardStats;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Executes a composite merge: primary format first, then secondaries using the
 * row-ID mapping from the primary. Stateless — all state comes from the
 * {@link MergePlan} and the merger map.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMergeExecutor {

    private final Map<DataFormat, Merger> mergers;
    private final CompositeShardStats stats;

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers, CompositeShardStats stats) {
        this.mergers = Map.copyOf(mergers);
        this.stats = stats;
    }

    /**
     * Executes the merge described by the plan.
     *
     * @param plan the pre-validated merge plan
     * @return the combined merge result across all formats
     */
    public MergeResult execute(MergePlan plan) {
        long startNanos = System.nanoTime();
        List<FormatMergeResult> completed = new ArrayList<>();
        try {
            long primaryStart = System.nanoTime();
            FormatMergeResult primaryResult = mergeFormat(plan, plan.primaryFormat(), null);
            stats.getOrCreateFormatStats(plan.primaryFormat().name())
                .addMergeTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - primaryStart));
            completed.add(primaryResult);

            RowIdMapping mapping = plan.hasSecondaries()
                ? primaryResult.rowIdMappingOpt()
                    .orElseThrow(() -> new IllegalStateException("Primary merge did not produce row-ID mapping required by secondaries"))
                : null;

            for (DataFormat secondary : plan.secondaryFormats()) {
                long secStart = System.nanoTime();
                try {
                    FormatMergeResult secResult = mergeFormat(plan, secondary, mapping);
                    stats.getOrCreateFormatStats(secondary.name())
                        .addMergeTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - secStart));
                    completed.add(secResult);
                } catch (Exception e) {
                    stats.getOrCreateFormatStats(secondary.name()).incMergeFailures();
                    throw e;
                }
            }

            // Count total input segments across all formats
            long inputCount = 0;
            for (DataFormat format : plan.filesByFormat().keySet()) {
                inputCount += plan.filesFor(format).size();
            }
            stats.addMergeSegmentsInputTotal(inputCount);

            return toMergeResult(completed, mapping);
        } catch (Exception e) {
            completed.forEach(FormatMergeResult::cleanup);
            if (e instanceof RuntimeException re) throw re;
            throw new UncheckedIOException((IOException) e);
        } finally {
            stats.incMergeTotal();
            stats.addMergeTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
        }
    }

    private FormatMergeResult mergeFormat(MergePlan plan, DataFormat format, RowIdMapping mapping) throws IOException {
        Merger merger = mergers.get(format);
        List<WriterFileSet> files = plan.filesFor(format);
        List<Segment> segments = new ArrayList<>();
        for (WriterFileSet wfs : files) {
            segments.add(Segment.builder(wfs.writerGeneration()).addSearchableFiles(format, wfs).build());
        }
        MergeResult result = merger.merge(new MergeInput(segments, mapping, plan.mergedWriterGeneration()));
        return new FormatMergeResult(format, result.getMergedWriterFileSetForDataformat(format), result.rowIdMapping().orElse(null));
    }

    private static MergeResult toMergeResult(List<FormatMergeResult> results, RowIdMapping mapping) {
        Map<DataFormat, WriterFileSet> merged = new HashMap<>();
        for (FormatMergeResult r : results) {
            merged.put(r.format(), r.mergedFiles());
        }
        return new MergeResult(merged, mapping);
    }
}
