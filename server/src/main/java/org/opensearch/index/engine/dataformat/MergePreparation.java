/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * The product of {@link Merger#prepareMerge}: a frozen {@link LiveDocs} view plus ownership of
 * whatever state the format pinned to freeze it (for Lucene, the merge readers and their file
 * references).
 *
 * <p>Mirrors {@link MergeResult} for the prepare phase. {@link Closeable} because preparing pins
 * resources: {@link #close()} releases anything the subsequent {@link Merger#merge} did not
 * consume, and must be idempotent and a no-op once {@code merge()} has taken the prepared state.
 * Callers hold the preparation in a try-with-resources around the merge so the release is
 * unconditional on the failure path without an explicit abort call.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface MergePreparation extends Closeable {

    /** Preparation that froze nothing: all rows alive, nothing to release. */
    MergePreparation EMPTY = new MergePreparation() {
        @Override
        public LiveDocs liveDocs() {
            return LiveDocs.ALL_ALIVE;
        }

        @Override
        public void close() {
            // nothing pinned
        }
    };

    /**
     * The frozen per-segment live-docs view. Never {@code null}; {@link LiveDocs#ALL_ALIVE} when
     * the preparing format has no deletes on the merge inputs.
     */
    LiveDocs liveDocs();

    /**
     * Releases prepared state the merge did not consume. Idempotent; a no-op after the format's
     * {@link Merger#merge} has taken the prepared state.
     */
    @Override
    void close() throws IOException;
}
