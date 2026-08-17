/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
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
    /**
     * Per-storage-format mergers for auxiliary (side-table) roles, used in preference to {@link #mergers}
     * when merging a side table. Engine-4's element index registers one here (keyed by the Lucene
     * format) because it must be merged differently from the documents — see
     * {@link org.opensearch.index.engine.dataformat.IndexingExecutionEngine#getAuxiliaryMerger}.
     */
    private final Map<DataFormat, Merger> auxiliaryMergers;

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers) {
        this(mergers, Map.of());
    }

    public CompositeMergeExecutor(Map<DataFormat, Merger> mergers, Map<DataFormat, Merger> auxiliaryMergers) {
        this.mergers = Map.copyOf(mergers);
        this.auxiliaryMergers = Map.copyOf(auxiliaryMergers);
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

    /**
     * Merges the side tables that were selected alongside the documents, returning them as one
     * segment.
     *
     * <p>Each side table is merged by its <em>storage</em> format's own merger — a nested child table
     * is parquet, merged by the parquet merger exactly as the parent's parquet file is. Only the keys
     * differ: the {@link MergeInput} is keyed by the storage format because that is what the merger
     * looks its inputs up by, while the returned {@link Segment} is keyed by the auxiliary format
     * name because that is what the catalog holds. See {@link AuxiliaryMergeGroup}.
     *
     * <p>The merged side table takes the generation derived from the merged document generation
     * ({@link AuxiliaryDataFormat#generationFor}), which keeps the pairing derivable through the
     * merge: the next merge can find this side table from its documents the same way this one did.
     *
     * <p><b>Foreign keys are not rewritten here.</b> {@code documentMapping} is passed through to the
     * merger so the hook exists where it belongs, but no merger applies it to a column yet. A side
     * table whose foreign key is a parent <em>row position</em> therefore still points at pre-merge
     * positions after this runs — the remaining half of the work, tracked as Phase 4b in
     * {@code design/nested-field-support/10-poc-write-path-plan.md}. A side table whose foreign key
     * is a stable parent value (e.g. {@code _seq_no}) needs no rewrite and is already correct.
     *
     * @param groups                  the side tables to merge, one entry per auxiliary format
     * @param mergedWriterGeneration  the generation of the merged document segment
     * @param documentMapping         the row-ID mapping the document merge produced, may be null
     * @return the merged side tables as a single auxiliary segment, or empty if there was nothing to
     *         merge
     */
    public List<Segment> executeAuxiliary(List<AuxiliaryMergeGroup> groups, long mergedWriterGeneration, RowIdMapping documentMapping) {
        List<AuxiliaryMergeGroup> nonEmpty = groups.stream().filter(g -> g.files().isEmpty() == false).toList();
        if (nonEmpty.isEmpty()) {
            return List.of();
        }
        assertSingleRole(nonEmpty);

        long auxiliaryGeneration = AuxiliaryDataFormat.generationFor(mergedWriterGeneration);
        List<FormatMergeResult> completed = new ArrayList<>();
        try {
            Segment.Builder mergedSegment = Segment.builder(auxiliaryGeneration);
            for (AuxiliaryMergeGroup group : nonEmpty) {
                // A side table may need a merger distinct from the one that merges the documents of the
                // same storage format — Engine-4's element index (Lucene) is merged from its own
                // directories with a __parent_row__ rewrite, not through the shard's shared writer. Use
                // the auxiliary merger when the storage format provides one; else fall back.
                Merger merger = auxiliaryMergers.getOrDefault(group.storageFormat(), mergers.get(group.storageFormat()));
                if (merger == null) {
                    throw new IllegalStateException(
                        "Side table ["
                            + group.auxiliaryFormatName()
                            + "] needs the merger of its storage format ["
                            + group.storageFormat().name()
                            + "], which this composite does not provide"
                    );
                }
                List<Segment> inputs = new ArrayList<>(group.files().size());
                for (WriterFileSet wfs : group.files()) {
                    inputs.add(Segment.builder(wfs.writerGeneration()).addSearchableFiles(group.storageFormat(), wfs).build());
                }
                MergeResult result = merger.merge(new MergeInput(inputs, documentMapping, auxiliaryGeneration));
                WriterFileSet mergedFiles = result.getMergedWriterFileSetForDataformat(group.storageFormat());
                if (mergedFiles == null) {
                    throw new IllegalStateException(
                        "Side table ["
                            + group.auxiliaryFormatName()
                            + "] merge over "
                            + group.files().size()
                            + " file sets returned no output for storage format ["
                            + group.storageFormat().name()
                            + "]"
                    );
                }
                completed.add(new FormatMergeResult(group.storageFormat(), mergedFiles, null));
                mergedSegment.addSearchableFiles(group.auxiliaryFormatName(), mergedFiles);
            }
            return List.of(mergedSegment.build());
        } catch (Exception e) {
            completed.forEach(FormatMergeResult::cleanup);
            if (e instanceof RuntimeException re) throw re;
            throw new UncheckedIOException((IOException) e);
        }
    }

    /**
     * Rejects a merge spanning more than one side table role.
     *
     * <p>All side tables of one role share a segment, the way a document segment holds its parquet
     * and Lucene files together. Two roles cannot: both would derive the same generation from the
     * merged document generation, and a generation identifies a segment. Supporting a second role
     * means giving the offset scheme room for it — see {@link AuxiliaryDataFormat#GENERATION_OFFSET}.
     */
    private static void assertSingleRole(List<AuxiliaryMergeGroup> groups) {
        String role = null;
        for (AuxiliaryMergeGroup group : groups) {
            String groupRole = AuxiliaryDataFormat.roleOf(group.auxiliaryFormatName());
            if (role == null) {
                role = groupRole;
            } else if (role.equals(groupRole) == false) {
                throw new IllegalStateException(
                    "Cannot merge side tables of different roles in one merge: found ["
                        + role
                        + "] and ["
                        + groupRole
                        + "]. One generation offset yields one auxiliary generation, so the two would collide."
                );
            }
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
