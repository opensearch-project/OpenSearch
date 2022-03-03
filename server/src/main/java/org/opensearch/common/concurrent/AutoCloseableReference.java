/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

public class AutoCloseableReference<T> implements AutoCloseable {

    private final T ref;
    private final Runnable onClose;
    private final AtomicBoolean closed = new AtomicBoolean();

    public AutoCloseableReference(T ref, Runnable onClose) {
        this.ref = ref;
        this.onClose = onClose;
    }

    public T get() {
        return ref;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }
}
