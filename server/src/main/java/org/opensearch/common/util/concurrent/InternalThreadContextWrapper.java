/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import java.util.Objects;

/**
 * Wrapper around the ThreadContext to expose methods to the core repo without
 * exposing them to plugins
 *
 * @opensearch.internal
 */
public class InternalThreadContextWrapper {
    private final ThreadContext threadContext;

    private InternalThreadContextWrapper(final ThreadContext threadContext) {
        this.threadContext = threadContext;
    }

    public static InternalThreadContextWrapper from(ThreadContext threadContext) {
        return new InternalThreadContextWrapper(threadContext);
    }

    public void markAsSystemContext() {
        Objects.requireNonNull(threadContext, "threadContext cannot be null");
        threadContext.markAsSystemContext();
    }
}
