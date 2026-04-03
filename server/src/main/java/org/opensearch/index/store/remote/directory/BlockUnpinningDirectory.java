/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link FilterDirectory} wrapper that tracks {@link AbstractBlockIndexInput}
 * instances opened during searchable snapshot restore so their file cache blocks
 * can be unpinned in bulk once the reader is fully opened.
 * <p>
 * Lifecycle (used inside {@code ReadOnlyEngine} constructor):
 * <ol>
 *   <li>Wrap the directory: {@code new BlockUnpinningDirectory(directory)}</li>
 *   <li>{@code DirectoryReader.open()} — every {@link #openInput} call tracks
 *       the returned input; slices and clones are tracked automatically via
 *       the {@code onClone} callback.</li>
 *   <li>{@link #unpinAndStopTracking()} — releases all held blocks (LRU-evictable)
 *       and disables further tracking.</li>
 * </ol>
 * <p>
 * Concurrency: {@link #openInput} may be called from multiple threads during
 * parallel segment opening, so {@code tracked} is a {@link ConcurrentLinkedQueue}
 * for lock-free concurrent adds. {@link #unpinAndStopTracking()} runs after
 * {@code DirectoryReader.open()} returns (join barrier).
 *
 * @opensearch.internal
 */
public final class BlockUnpinningDirectory extends FilterDirectory {

    private Queue<AbstractBlockIndexInput> tracked = new ConcurrentLinkedQueue<>();

    public BlockUnpinningDirectory(Directory in) {
        super(in);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput input = super.openInput(name, context);
        if (tracked != null && input instanceof AbstractBlockIndexInput blockInput) {
            blockInput.setOnClone(this::track);
            track(blockInput);
        }
        return input;
    }

    private void track(AbstractBlockIndexInput input) {
        if (tracked != null) {
            tracked.add(input);
        }
    }

    /**
     * Unpins all file cache blocks held by inputs opened through this directory,
     * then stops tracking. Safe to call multiple times.
     */
    public void unpinAndStopTracking() {
        if (tracked == null) return;
        for (AbstractBlockIndexInput input : tracked) {
            input.unpinBlock();
        }
        tracked = null;
    }
}
