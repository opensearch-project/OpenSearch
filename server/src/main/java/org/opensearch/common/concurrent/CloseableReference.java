/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.concurrent;

import org.opensearch.common.CheckedRunnable;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloseableReference<T> implements Closeable {

    private final T ref;
    private final CheckedRunnable<IOException> onClose;
    private final AtomicBoolean closed = new AtomicBoolean();

    public CloseableReference(T ref, CheckedRunnable<IOException> onClose) {
        this.ref = ref;
        this.onClose = onClose;
    }

    public T get() {
        return ref;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            onClose.run();
        }
    }
}
