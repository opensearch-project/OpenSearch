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

import org.opensearch.common.util.concurrent.RefCounted;

/**
 * Adapter class that enables a {@link RefCounted} implementation to function like an {@link AutoCloseable}.
 * The {@link #close()} API invokes {@link RefCounted#decRef()} and ensures idempotency using a {@link OneWayGate}.
 *
 * @opensearch.internal
 */
public class AutoCloseableRefCounted<T extends RefCounted> implements AutoCloseable {

    private final T ref;
    private final OneWayGate gate;

    public AutoCloseableRefCounted(T ref) {
        this.ref = ref;
        gate = new OneWayGate();
    }

    public T get() {
        return ref;
    }

    @Override
    public void close() {
        if (gate.close()) {
            ref.decRef();
        }
    }
}
