/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An {@link Iterator} that holds a resource (e.g. an open spill-file stream) and must be
 * {@link #close() closed} when iteration finishes or is abandoned.
 *
 * <p>Used by the hash-shuffle consumer to drain a partition's chunks ONE AT A TIME — when the
 * chunks were spilled to disk, the iterator streams them back from the file without ever holding
 * the whole partition in heap. Callers MUST close it (try-with-resources) so the underlying file
 * handle is released even on partial iteration.
 *
 * @param <T> element type
 * @opensearch.internal
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    /** Closes the underlying resource. Idempotent. Never throws checked exceptions. */
    @Override
    void close();
}
