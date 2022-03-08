/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.concurrent;

/**
 * Decorator class that wraps an object reference with a {@link Runnable} that is
 * invoked when {@link #close()} is called. The internal {@link OneWayGate} instance ensures
 * that this is invoked only once. See also {@link GatedCloseable}
 */
public class GatedAutoCloseable<T> implements AutoCloseable {

    private final T ref;
    private final Runnable onClose;
    private final OneWayGate gate;

    public GatedAutoCloseable(T ref, Runnable onClose) {
        this.ref = ref;
        this.onClose = onClose;
        gate = new OneWayGate();
    }

    public T get() {
        return ref;
    }

    @Override
    public void close() {
        if (gate.close()) {
            onClose.run();
        }
    }
}
