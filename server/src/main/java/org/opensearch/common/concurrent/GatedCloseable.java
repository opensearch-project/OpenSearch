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

import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.annotation.PublicApi;

import java.io.Closeable;
import java.io.IOException;

/**
 * Decorator class that wraps an object reference with a {@link CheckedRunnable} that is
 * invoked when {@link #close()} is called. The internal {@link OneWayGate} instance ensures
 * that this is invoked only once. See also {@link AutoCloseableRefCounted}
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GatedCloseable<T> implements Closeable {

    private final T ref;
    private final CheckedRunnable<IOException> onClose;
    private final OneWayGate gate;

    public GatedCloseable(T ref, CheckedRunnable<IOException> onClose) {
        this.ref = ref;
        this.onClose = onClose;
        gate = new OneWayGate();
    }

    public T get() {
        return ref;
    }

    @Override
    public void close() throws IOException {
        if (gate.close()) {
            onClose.run();
        }
    }
}
