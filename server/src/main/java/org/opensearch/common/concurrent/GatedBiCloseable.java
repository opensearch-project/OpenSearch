/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.concurrent;

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.annotation.PublicApi;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A two-path variant of {@link GatedCloseable} that executes different actions on close
 * depending on whether {@link #markSuccess()} was called.
 * <p>
 * If {@code markSuccess()} is called before {@code close()}, the {@code onSuccess} action runs.
 * Otherwise, the {@code onFailure} action runs. Close is idempotent — only the first call has effect.
 * <p>
 * Typical usage:
 * <pre>{@code
 * try (GatedBiCloseable<Resource> handle = acquireResource()) {
 *     doWork(handle.get());
 *     handle.markSuccess();
 * }
 * // close() runs onSuccess if markSuccess() was called, onFailure otherwise
 * }</pre>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public class GatedBiCloseable<T> implements Closeable {

    private final T ref;
    private final CheckedRunnable<IOException> onSuccess;
    private final CheckedRunnable<IOException> onFailure;
    private final AtomicBoolean succeeded = new AtomicBoolean(false);
    private final OneWayGate gate = new OneWayGate();

    public GatedBiCloseable(T ref, CheckedRunnable<IOException> onSuccess, CheckedRunnable<IOException> onFailure) {
        this.ref = ref;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
    }

    public T get() {
        return ref;
    }

    /**
     * Marks this handle as successful. Must be called before {@link #close()}
     * for the success action to execute.
     */
    public void markSuccess() {
        succeeded.set(true);
    }

    @Override
    public void close() throws IOException {
        if (gate.close()) {
            if (succeeded.get()) {
                onSuccess.run();
            } else {
                onFailure.run();
            }
        }
    }
}
