/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An {@link IndexWriter} subclass that exposes Lucene's internal {@code merge(OneMerge)}
 * path for use by the pluggable data format merge infrastructure.
 *
 * <p>The internal merge path handles the full segment lifecycle including reference-counted
 * file cleanup via {@code IndexFileDeleter}. If the merge fails, old segments are preserved
 * and the partially-written merged segment is cleaned up — providing a safe rollback mechanism.
 *
 * <p>This class is placed in the {@code org.apache.lucene.index} package to access
 * package-private fields on {@link MergePolicy.OneMerge} required for merge registration.
 *
 * <p>The {@link IndexWriterConfig} used to construct this writer must set a
 * {@link SerialMergeScheduler} to avoid the {@link ConcurrentMergeScheduler} thread
 * assertion in {@code wrapForMerge}, since pluggable data format merges run on the
 * engine's own merge thread pool rather than Lucene's {@code MergeThread}.
 *
 * <h2>Coordination with engine refreshes</h2>
 *
 * <p>This class itself does not take any engine-level locks. Coordination with the engine's
 * refresh path is layered on top by installing a {@code MergedSegmentWarmer} on the
 * {@link IndexWriterConfig} (see {@code LuceneCommitter}). The warmer runs between
 * {@code mergeMiddle} and {@code commitMerge}, at a point where the {@link IndexWriter}
 * monitor is <em>not</em> held, and acquires the engine's refresh lock. This establishes the
 * ordering {@code refreshLock → IndexWriter monitor} on the merge thread, matching the order
 * used by the engine's refresh path (which takes the refresh lock before calling
 * {@code addIndexes}). The expensive {@code mergeMiddle} phase therefore runs without holding
 * the refresh lock, and only the short {@code commitMerge} window is serialized against
 * refreshes.
 *
 * <h2>Two-phase merge for cross-format snapshot consistency</h2>
 *
 * <p>For composite engines where Lucene is a secondary format, the live-docs snapshot used by
 * the primary-format merger must match what Lucene will physically drop in {@code mergeMiddle}.
 * Without coordination, the snapshot is captured later than the primary's bitmap and a delete
 * arriving in between produces a row-count mismatch across formats. To avoid this,
 * {@link #prepareMerge(PreparableOneMerge, long)} is invoked first — it runs Lucene's
 * {@code mergeInit} and then explicitly populates the {@link PreparableOneMerge}'s merge readers
 * (which causes a copy-on-write flip on each source segment's live docs). The frozen
 * {@code MergeReader.hardLiveDocs} can then be read by callers to construct the cross-format
 * bitmap. The subsequent call to {@link #executeMerge(MergePolicy.OneMerge, long)} reuses the
 * prepared merge object so {@code mergeMiddle} proceeds with the same snapshot. Callers that
 * do not need the two-phase split simply call {@link #executeMerge}.
 *
 * @opensearch.experimental
 */
public class MergeIndexWriter extends IndexWriter {

    private final Map<Long, PreparableOneMerge> preparedMerges = new ConcurrentHashMap<>();

    public MergeIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
        super(d, conf);
    }

    /**
     * Forces any in-memory live-docs deletes for the given segment to be persisted to
     * disk as a {@code .liv} file. Required after a merge whose
     * {@code carryOverHardDeletes} produced new dead bits on the merged segment, so
     * the catalog publish that follows sees the {@code .liv} file in
     * {@link SegmentCommitInfo#files()} and stays consistent with what readers will
     * see on subsequent refresh.
     *
     * <p>Without this, the merged segment's {@code SegmentCommitInfo} reports
     * {@code hasDeletions() == false} immediately after merge (because
     * {@code ReaderPool} writes live-docs lazily on flush), so the catalog records
     * a file set without {@code .liv}; later when a flush triggers the {@code .liv}
     * write, the leaf has one extra file and the catalog/leaf mismatch surfaces as
     * {@code "Catalog segment ... has no matching Lucene leaf"} on the next refresh.
     */
    public synchronized void flushLiveDocsForMergedSegment(SegmentCommitInfo mergedInfo) throws IOException {
        ReadersAndUpdates rld = getPooledInstance(mergedInfo, false);
        if (rld == null) {
            return;
        }
        rld.writeLiveDocs(getDirectory());
    }

    /**
     * Prepares a merge by running {@code mergeInit} and populating its merge readers — which
     * triggers a copy-on-write flip on each source segment's live docs, freezing the snapshot.
     * Files referenced by each opened reader are pinned via the same per-reader
     * {@code deleter.incRef} hook Lucene's own {@code mergeMiddle} uses, so they survive the
     * gap between this call and the eventual {@link #executeMerge} call. The matching
     * {@code decRef} happens through Lucene's normal {@code closeMergeReaders} cleanup once
     * the merge runs (success or failure).
     *
     * <p>After this returns, callers can read the frozen snapshot through
     * {@code oneMerge.getMergeReader().get(i).hardLiveDocs} to build a bitmap that matches
     * exactly what {@code mergeMiddle} will use for physical drops.
     *
     * <p>{@link #executeMerge(MergePolicy.OneMerge, long)} must subsequently be invoked with
     * the same generation; it picks up the prepared merge from an internal map and runs the
     * rest of the merge. If the parquet phase fails before {@code executeMerge}, the caller
     * must invoke {@link #abortPreparedMerge(long)} to release reader-pool and deleter refs.
     *
     * <p>If this method itself fails partway through {@code initMergeReaders}, it walks
     * Lucene's standard abort path ({@code setAborted} then {@code merge}) so any readers
     * that were opened before the failure get closed, their file refs decremented, and
     * their {@code setIsMerging} flags cleared. Only the original exception is rethrown;
     * any abort-path failure is suppressed.
     *
     * @param oneMerge        the merge to prepare
     * @param mergeGeneration the writer generation for the merged output segment; used as the
     *                        coordination key for {@link #executeMerge} and
     *                        {@link #abortPreparedMerge}
     */
    public void prepareMerge(PreparableOneMerge oneMerge, long mergeGeneration) throws IOException {
        assert preparedMerges.containsKey(mergeGeneration) == false : "prepareMerge invoked for generation "
            + mergeGeneration
            + " that already has a prepared merge";
        synchronized (this) {
            oneMerge.mergeGen = mergeGeneration;
            oneMerge.isExternal = false;
            oneMerge.maxNumSegments = -1;
            oneMerge.registerDone = true;
        }
        // mergeInit asserts !Thread.holdsLock(this); waitApplyForMerge resolves any pending
        // delete packets covering source segments, _mergeInit creates the output SegmentInfo.
        mergeInit(oneMerge);

        try {
            IOContext ctx = IOContext.merge(oneMerge.getStoreMergeInfo());
            oneMerge.initMergeReaders(sci -> {
                ReadersAndUpdates rld = getPooledInstance(sci, true);
                rld.setIsMerging();
                synchronized (this) {
                    // Per-reader incRef matches what Lucene's own mergeMiddle does. The matching
                    // decRef fires from closeMergeReaders during executeMerge cleanup. Per-reader
                    // accounting (not bulk incRefDeleter) means partial-failure cleanup via
                    // Lucene's standard abort path balances correctly: only opened readers were
                    // incRef'd, only opened readers get decRef'd.
                    return rld.getReaderForMerge(ctx, mr -> {
                        SegmentInfos perReader = new SegmentInfos(getConfig().getIndexCreatedVersionMajor());
                        perReader.add(mr.reader.getSegmentInfo());
                        incRefDeleter(perReader);
                    });
                }
            });
            preparedMerges.put(mergeGeneration, oneMerge);
        } catch (Throwable t) {
            // Ride Lucene's own abort path: setAborted + merge fast-fails inside mergeMiddle's
            // checkAborted and the surrounding finally runs closeMergeReaders, which closes any
            // readers we managed to open, decRefs their files (balancing our per-reader incRef),
            // and clears the setIsMerging flags. We swallow the expected MergeAbortedException
            // and suppress any other cleanup failure into the original throwable.
            try {
                oneMerge.setAborted();
                merge(oneMerge);
            } catch (MergePolicy.MergeAbortedException expected) {
                // expected — cleanup ran in merge()'s finally
            } catch (Throwable suppress) {
                t.addSuppressed(suppress);
            }
            throw t;
        }
    }

    /**
     * Returns the prepared merge for {@code mergeGeneration} if one exists, removing it from
     * the internal map. Returns {@code null} if no merge was prepared (or if it has already
     * been consumed). The caller is responsible for invoking {@link #executeMerge} with the
     * returned OneMerge — its reader-pool and deleter references must be released by the
     * surrounding merge cleanup.
     */
    public PreparableOneMerge takePreparedMerge(long mergeGeneration) {
        return preparedMerges.remove(mergeGeneration);
    }

    /**
     * Aborts a prepared merge and releases its reader-pool and deleter references.
     * Callers must invoke this if the parquet (primary) phase fails between
     * {@link #prepareMerge} and the call to {@link #executeMerge} that would have consumed
     * the prepared state.
     *
     * <p>Marks the prepared merge as aborted and invokes {@link IndexWriter#merge}; the
     * inner {@code mergeMiddle.checkAborted()} fast-fails with a
     * {@link MergePolicy.MergeAbortedException} which we swallow here, while the
     * surrounding finally block in {@code merge()} releases the reader-pool and deleter
     * references that {@link #prepareMerge} took.
     */
    public void abortPreparedMerge(long mergeGeneration) throws IOException {
        PreparableOneMerge prepared = preparedMerges.remove(mergeGeneration);
        if (prepared == null) {
            return;
        }
        prepared.setAborted();
        try {
            merge(prepared);
        } catch (MergePolicy.MergeAbortedException expected) {
            // Cleanup ran inside merge()'s finally block.
        }
    }

    /**
     * Executes a merge using Lucene's internal merge path which handles:
     * <ol>
     *   <li>mergeInit — creates output segment info, increments file references</li>
     *   <li>mergeMiddle — reads sources via wrapForMerge, applies IndexSort via MultiSorter,
     *       writes merged segment</li>
     *   <li>commitMerge — removes old segments from live list, decrements file references</li>
     *   <li>mergeFinish — cleans up merge tracking state</li>
     * </ol>
     *
     * <p>If the merge fails at any point, old segments are preserved and the partially-written
     * merged segment is cleaned up by IndexFileDeleter's reference counting.
     *
     * <p>Duplicate segment prevention is handled by the caller; this method does not
     * validate against concurrent merges on the same segments.
     *
     * <p>Refresh-lock coordination is handled by the {@code MergedSegmentWarmer} installed on
     * this writer's {@link IndexWriterConfig} — see the class Javadoc for details.
     *
     * @param oneMerge        the merge to execute. May be a {@link PreparableOneMerge} returned
     *                        by {@link #takePreparedMerge} (in which case {@code mergeMiddle}
     *                        reuses the captured snapshot) or a fresh OneMerge (in which case
     *                        {@code mergeMiddle} captures its own snapshot inline).
     * @param mergeGeneration the writer generation for the merged output segment
     * @throws IOException if the merge fails
     */
    public void executeMerge(MergePolicy.OneMerge oneMerge, long mergeGeneration) throws IOException {
        boolean alreadyRegistered = oneMerge instanceof PreparableOneMerge p && p.readersInitialized();
        if (alreadyRegistered == false) {
            synchronized (this) {
                oneMerge.mergeGen = mergeGeneration;
                oneMerge.isExternal = false;
                oneMerge.maxNumSegments = -1;
                oneMerge.registerDone = true;
            }
        }
        // merge() must be called without holding the lock — mergeInit asserts !Thread.holdsLock(this).
        // Refresh-lock acquisition happens inside the MergedSegmentWarmer configured on this writer,
        // which fires between mergeMiddle and commitMerge while the IW monitor is not held. This
        // matches the refresh path's lock order (refreshLock → IW monitor) and avoids any inversion.
        merge(oneMerge);
    }

    @Override
    protected void mergeSuccess(MergePolicy.OneMerge merge) {
        // TODO update this for lucene as a primary engine
        // https://github.com/opensearch-project/OpenSearch/issues/21505
        super.mergeSuccess(merge);
    }
}
