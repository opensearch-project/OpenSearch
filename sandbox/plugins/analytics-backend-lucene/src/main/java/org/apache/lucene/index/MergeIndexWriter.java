/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

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
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeIndexWriter extends IndexWriter {

    public MergeIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
        super(d, conf);
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
     * @param oneMerge       the merge to execute
     * @param mergeGeneration the writer generation for the merged output segment
     * @throws IOException if the merge fails
     */
    public void executeMerge(MergePolicy.OneMerge oneMerge, long mergeGeneration) throws IOException {
        synchronized (this) {
            oneMerge.mergeGen = mergeGeneration;
            oneMerge.isExternal = false;
            oneMerge.maxNumSegments = -1;
            oneMerge.registerDone = true;
        }
        // merge() must be called without holding the lock — mergeInit asserts !Thread.holdsLock(this)
        merge(oneMerge);
    }
}
