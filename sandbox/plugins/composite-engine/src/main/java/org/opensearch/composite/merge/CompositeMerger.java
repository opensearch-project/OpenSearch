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
import org.opensearch.index.engine.dataformat.MergePreparation;
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
    /** The {@link Merger} of the one secondary whose {@link Merger#providesMergeLiveDocs()} is true, or {@code null}. */
    private final Merger liveDocsProducer;

    public CompositeMerger(CompositeIndexingExecutionEngine engine, CompositeDataFormat compositeDataFormat) {
        this.primaryFormat = compositeDataFormat.getPrimaryDataFormat();
        this.secondaryFormats = resolveSecondaryFormats(compositeDataFormat, primaryFormat);
        this.executor = new CompositeMergeExecutor(buildMergerMap(engine));
        this.statsTracker = engine.statsTracker();
        this.liveDocsProducer = resolveLiveDocsProducer(secondaryFormats, executor);
    }

    /**
     * Two-phase merge: phase 1 freezes the live-docs producer's snapshot via {@link #prepareMerge};
     * phase 2 runs primary → secondaries with that frozen bitmap. The {@link MergePreparation} is
     * held in a try-with-resources so the producer's pinned state is released on every exit path
     * where its {@code merge()} did not consume it — no explicit abort call for callers to forget.
     * The bitmap from prepareMerge overrides any {@link MergeInput#liveDocs()} the caller passed in.
     */
    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        // recordOutcome: time always, merge_total on success, merge_failures on throw.
        return StatsRecorder.recordOutcome(() -> {
            try (MergePreparation preparation = prepareMerge(mergeInput)) {
                LiveDocs frozenLiveDocs = preparation.liveDocs();
                assert frozenLiveDocs != null : "merger returned null live-docs";
                assert assertLiveDocsShape(mergeInput.segments(), frozenLiveDocs) : "live-docs shape doesn't match segment row counts";

                Map<DataFormat, List<WriterFileSet>> filesByFormat = extractFilesByFormat(mergeInput.segments());
                MergePlan plan = new MergePlan(
                    mergeInput.newWriterGeneration(),
                    primaryFormat,
                    secondaryFormats,
                    filesByFormat,
                    frozenLiveDocs
                );
                return executor.execute(plan);
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
    public boolean providesMergeLiveDocs() {
        return liveDocsProducer != null;
    }

    /**
     * Delegates to the single resolved live-docs producer. Secondaries that do not provide live
     * docs are never asked to prepare, so they have nothing to release either.
     */
    @Override
    public MergePreparation prepareMerge(MergeInput mergeInput) throws IOException {
        if (liveDocsProducer == null) {
            return MergePreparation.EMPTY;
        }
        MergePreparation preparation = liveDocsProducer.prepareMerge(mergeInput);
        return preparation == null ? MergePreparation.EMPTY : preparation;
    }

    /**
     * Picks the secondary that owns delete state for merges. Mirrors the registry's single
     * delete-execution-engine rule: opt-in via {@link Merger#providesMergeLiveDocs()}, at most one,
     * and a second producer is a configuration error rather than something to silently drop.
     */
    private static Merger resolveLiveDocsProducer(List<DataFormat> secondaries, CompositeMergeExecutor executor) {
        List<DataFormat> producers = new ArrayList<>();
        for (DataFormat secondary : secondaries) {
            Merger merger = executor.getMerger(secondary);
            if (merger != null && merger.providesMergeLiveDocs()) {
                producers.add(secondary);
            }
        }
        if (producers.size() > 1) {
            throw new IllegalStateException(
                "Multiple secondary formats provide merge live-docs, expected at most one but found "
                    + producers.stream().map(DataFormat::name).toList()
            );
        }
        return producers.isEmpty() ? null : executor.getMerger(producers.get(0));
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
