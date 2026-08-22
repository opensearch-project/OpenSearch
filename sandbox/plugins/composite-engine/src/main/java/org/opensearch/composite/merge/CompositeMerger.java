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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.LiveDocs;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
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
        this.executor = new CompositeMergeExecutor(buildMergerMap(engine));
        this.statsTracker = engine.statsTracker();
    }

    /**
     * Two-phase merge: phase 1 freezes each secondary's snapshot via {@link #prepareMerge};
     * phase 2 runs primary → secondaries with that frozen bitmap. On failure between phases,
     * {@link #abortPreparedMerge} releases prepared state. The bitmap from prepareMerge
     * overrides any {@link MergeInput#liveDocs()} the caller passed in.
     */
    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        // recordOutcome: time always, merge_total on success, merge_failures on throw.
        return StatsRecorder.recordOutcome(() -> {
            LiveDocs frozenLiveDocs = prepareMerge(mergeInput);
            assert frozenLiveDocs != null : "merger returned null live-docs";
            assert assertLiveDocsShape(mergeInput.segments(), frozenLiveDocs) : "live-docs shape doesn't match segment row counts";

            try {
                Map<DataFormat, List<WriterFileSet>> filesByFormat = extractFilesByFormat(mergeInput.segments());
                MergePlan plan = new MergePlan(
                    mergeInput.newWriterGeneration(),
                    primaryFormat,
                    secondaryFormats,
                    filesByFormat,
                    frozenLiveDocs
                );
                return executor.execute(plan);
            } catch (Throwable t) {
                try {
                    abortPreparedMerge(mergeInput);
                } catch (Throwable suppress) {
                    t.addSuppressed(suppress);
                }
                throw t;
            }
        }, statsTracker::addMergeTimeMillis, statsTracker::incMergeTotal, statsTracker::incMergeFailures);
    }

    /** Verifies the bitmap has at least as many bits as the segment's row count. */
    private static boolean assertLiveDocsShape(List<Segment> segments, LiveDocs liveDocs) {
        for (Segment seg : segments) {
            long[] bits = liveDocs.packedBits(seg.generation());
            if (bits == null) {
                continue;
            }
            long capacity = (long) bits.length * 64L;
            long expected = totalRowsForSegment(seg);
            assert capacity >= expected : "live-docs bitmap for gen="
                + seg.generation()
                + " has "
                + capacity
                + " bits but segment has "
                + expected
                + " rows";
        }
        return true;
    }

    private static long totalRowsForSegment(Segment seg) {
        long total = 0L;
        for (var wfs : seg.dfGroupedSearchableFiles().values()) {
            total = Math.max(total, wfs.numRows());
        }
        return total;
    }

    @Override
    public LiveDocs prepareMerge(MergeInput mergeInput) throws IOException {
        // Drive prepare on every secondary so each freezes its own state. Today returns
        // the first non-empty bitmap;
        LiveDocs firstNonEmpty = LiveDocs.ALL_ALIVE;
        for (DataFormat secondary : secondaryFormats) {
            Merger merger = executor.getMerger(secondary);
            if (merger == null) continue;
            LiveDocs partial = merger.prepareMerge(mergeInput);
            if (partial != null && partial.allAlive() == false && firstNonEmpty.allAlive()) {
                firstNonEmpty = partial;
            }
        }
        return firstNonEmpty;
    }

    @Override
    public void abortPreparedMerge(MergeInput mergeInput) throws IOException {
        IOException firstFailure = null;
        for (DataFormat secondary : secondaryFormats) {
            Merger merger = executor.getMerger(secondary);
            if (merger == null) continue;
            try {
                merger.abortPreparedMerge(mergeInput);
            } catch (IOException e) {
                if (firstFailure == null) firstFailure = e;
                else firstFailure.addSuppressed(e);
            }
        }
        if (firstFailure != null) throw firstFailure;
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
