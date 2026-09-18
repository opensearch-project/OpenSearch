/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
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

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers) {
        this.mergers = Map.copyOf(mergers);
    }

    /**
     * Executes the merge described by the plan.
     *
     * @param plan the pre-validated merge plan
     * @return the combined merge result across all formats
     */
    public MergeResult execute(MergePlan plan) {
        List<FormatMergeResult> completed = new ArrayList<>();
        try {
            FormatMergeResult primaryResult = mergeFormat(plan, plan.primaryFormat(), null);
            completed.add(primaryResult);

            RowIdMapping mapping = plan.hasSecondaries()
                ? primaryResult.rowIdMappingOpt()
                    .orElseThrow(() -> new IllegalStateException("Primary merge did not produce row-ID mapping required by secondaries"))
                : null;

            for (DataFormat secondary : plan.secondaryFormats()) {
                FormatMergeResult secondaryResult = mergeFormat(plan, secondary, mapping);
                // Verify secondary produced output when primary did
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() == null) {
                    throw new IllegalStateException(
                        "Primary format ["
                            + plan.primaryFormat().name()
                            + "] produced merged output but secondary format ["
                            + secondary.name()
                            + "] returned null — possible concurrent merge consumed segments"
                    );
                }
                // Verify secondary merged row count matches primary
                if (primaryResult.mergedFiles() != null && secondaryResult.mergedFiles() != null) {
                    long primaryRows = primaryResult.mergedFiles().numRows();
                    long secondaryRows = secondaryResult.mergedFiles().numRows();
                    if (primaryRows != secondaryRows) {
                        throw new IllegalStateException(
                            "Row count mismatch after merge: primary format ["
                                + plan.primaryFormat().name()
                                + "] has "
                                + primaryRows
                                + " rows but secondary format ["
                                + secondary.name()
                                + "] has "
                                + secondaryRows
                                + " rows"
                        );
                    }
                }
                completed.add(secondaryResult);
            }

            return toMergeResult(completed, mapping);
        } catch (Exception e) {
            completed.forEach(FormatMergeResult::cleanup);
            if (e instanceof RuntimeException re) throw re;
            throw new UncheckedIOException((IOException) e);
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
