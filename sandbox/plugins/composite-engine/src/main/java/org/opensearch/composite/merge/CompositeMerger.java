/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.composite.stats.CompositeShardStatsTracker;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.plugin.stats.StatsRecorder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A {@link Merger} that orchestrates composite merges across primary and secondary
 * data formats by delegating to {@link CompositeMergeExecutor}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMerger implements Merger {

    private final DataFormat primaryFormat;
    private final List<DataFormat> secondaryFormats;
    private final CompositeMergeExecutor executor;
    private final CompositeShardStatsTracker statsTracker;

    public CompositeMerger(CompositeIndexingExecutionEngine engine, CompositeDataFormat compositeDataFormat) {
        this.primaryFormat = compositeDataFormat.getPrimaryDataFormat();
        this.secondaryFormats = resolveSecondaryFormats(compositeDataFormat, primaryFormat);
        this.executor = new CompositeMergeExecutor(buildMergerMap(engine), buildAuxiliaryMergerMap(engine));
        this.statsTracker = engine.statsTracker();
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        // recordOutcome: time always, merge_total on success, merge_failures on throw.
        return StatsRecorder.recordOutcome(() -> {
            // A merge input can hold two kinds of segment: the documents the merge policy selected,
            // and the side tables that were selected with them. They are merged separately — the
            // document formats jointly (a shared row space, so the primary's row-ID mapping applies
            // to the secondaries), each side table on its own (its own row space). Splitting here
            // rather than merging blind is what stops a side table's files from being dropped: the
            // document path only ever asks for the formats it knows, so an unrecognised catalog key
            // would silently take its rows with it.
            List<Segment> documentSegments = new ArrayList<>();
            List<Segment> auxiliarySegments = new ArrayList<>();
            for (Segment segment : mergeInput.segments()) {
                (segment.isAuxiliaryOnly() ? auxiliarySegments : documentSegments).add(segment);
            }
            if (documentSegments.isEmpty()) {
                throw new IllegalStateException(
                    "Merge input at generation ["
                        + mergeInput.newWriterGeneration()
                        + "] holds only side tables. A side table merges alongside the documents it "
                        + "describes, never on its own — its generation and its foreign keys both derive from theirs."
                );
            }

            Map<DataFormat, List<WriterFileSet>> filesByFormat = extractFilesByFormat(documentSegments);
            MergePlan plan = new MergePlan(mergeInput.newWriterGeneration(), primaryFormat, secondaryFormats, filesByFormat);
            MergeResult documentResult = executor.execute(plan);

            if (auxiliarySegments.isEmpty()) {
                return documentResult;
            }
            RowIdMapping documentMapping = documentResult.rowIdMapping().orElse(null);
            List<Segment> mergedAuxiliaries = executor.executeAuxiliary(
                auxiliaryGroups(auxiliarySegments),
                mergeInput.newWriterGeneration(),
                documentMapping
            );
            return new MergeResult(documentResult.getMergedWriterFileSet(), documentMapping, mergedAuxiliaries);
        }, statsTracker::addMergeTimeMillis, statsTracker::incMergeTotal, statsTracker::incMergeFailures);
    }

    /**
     * Groups the side table files of every auxiliary segment by catalog format name, pairing each
     * with the format that will merge it.
     *
     * <p>Order is preserved as given, which is catalog order — the same order
     * {@link #extractFilesByFormat} collects the document files in. The two orders have to agree for
     * a positional foreign key to be rewritable at all.
     */
    private List<AuxiliaryMergeGroup> auxiliaryGroups(List<Segment> auxiliarySegments) {
        Map<String, List<WriterFileSet>> filesByAuxiliaryName = new LinkedHashMap<>();
        for (Segment segment : auxiliarySegments) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                filesByAuxiliaryName.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
            }
        }
        List<AuxiliaryMergeGroup> groups = new ArrayList<>(filesByAuxiliaryName.size());
        filesByAuxiliaryName.forEach(
            (auxiliaryName, files) -> groups.add(new AuxiliaryMergeGroup(auxiliaryName, resolveStorageFormat(auxiliaryName), files))
        );
        return List.copyOf(groups);
    }

    /**
     * Resolves the format that owns a side table's files, i.e. the one whose merger and file layout
     * it reuses. Matched by name against this composite's own formats, because the catalog holds only
     * names — the {@link org.opensearch.index.engine.dataformat.AuxiliaryDataFormat} instance itself
     * is not reachable from a deserialised segment.
     */
    private DataFormat resolveStorageFormat(String auxiliaryFormatName) {
        String storageName = DataFormat.storageNameOf(auxiliaryFormatName);
        if (primaryFormat.name().equals(storageName)) {
            return primaryFormat;
        }
        for (DataFormat secondary : secondaryFormats) {
            if (secondary.name().equals(storageName)) {
                return secondary;
            }
        }
        throw new IllegalStateException(
            "Side table ["
                + auxiliaryFormatName
                + "] sits in the storage of format ["
                + storageName
                + "], which this composite does not hold. Known formats: "
                + Stream.concat(Stream.of(primaryFormat), secondaryFormats.stream()).map(DataFormat::name).toList()
        );
    }

    private Map<DataFormat, List<WriterFileSet>> extractFilesByFormat(List<Segment> segments) {
        Set<DataFormat> allFormats = new LinkedHashSet<>();
        allFormats.add(primaryFormat);
        allFormats.addAll(secondaryFormats);

        Map<DataFormat, List<WriterFileSet>> filesByFormat = new LinkedHashMap<>();
        for (DataFormat format : allFormats) {
            List<WriterFileSet> files = new ArrayList<>();
            for (Segment segment : segments) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(format.name());
                if (wfs != null) {
                    files.add(wfs);
                }
            }
            filesByFormat.put(format, List.copyOf(files));
        }
        return filesByFormat;
    }

    private static List<DataFormat> resolveSecondaryFormats(CompositeDataFormat compositeDataFormat, DataFormat primaryFormat) {
        List<DataFormat> secondaries = new ArrayList<>();
        for (DataFormat format : compositeDataFormat.getDataFormats()) {
            if (format.equals(primaryFormat) == false) {
                secondaries.add(format);
            }
        }
        return List.copyOf(secondaries);
    }

    /**
     * Collects per-storage-format auxiliary (side-table) mergers from the delegates. Only formats that
     * provide a role-specific merger appear here; today that is the Lucene delegate, which returns an
     * element-index merger for the {@code nested} role. Keyed by the delegate's own data format, which
     * is the storage format side tables of that format resolve to.
     */
    private static Map<DataFormat, Merger> buildAuxiliaryMergerMap(CompositeIndexingExecutionEngine engine) {
        Map<DataFormat, Merger> map = new HashMap<>();
        List<IndexingExecutionEngine<?, ?>> delegates = new ArrayList<>();
        delegates.add(engine.getPrimaryDelegate());
        delegates.addAll(engine.getSecondaryDelegates());
        for (IndexingExecutionEngine<?, ?> delegate : delegates) {
            Merger auxMerger = delegate.getAuxiliaryMerger(AuxiliaryDataFormat.NESTED_CHILD_ROLE);
            if (auxMerger != null) {
                map.put(delegate.getDataFormat(), auxMerger);
            }
        }
        return Map.copyOf(map);
    }

    private static Map<DataFormat, Merger> buildMergerMap(CompositeIndexingExecutionEngine engine) {
        Map<DataFormat, Merger> map = new HashMap<>();

        Merger primaryMerger = engine.getPrimaryDelegate().getMerger();
        if (primaryMerger == null) {
            throw new IllegalStateException(
                "Primary format [" + engine.getPrimaryDelegate().getDataFormat().name() + "] does not provide a Merger"
            );
        }
        map.put(engine.getPrimaryDelegate().getDataFormat(), primaryMerger);

        for (IndexingExecutionEngine<?, ?> secondary : engine.getSecondaryDelegates()) {
            Merger merger = secondary.getMerger();
            if (merger == null) {
                throw new IllegalStateException("Secondary format [" + secondary.getDataFormat().name() + "] does not provide a Merger");
            }
            map.put(secondary.getDataFormat(), merger);
        }
        return Map.copyOf(map);
    }
}
