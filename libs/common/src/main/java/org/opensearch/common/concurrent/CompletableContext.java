/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.concurrent;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * A thread-safe completable context that allows listeners to be attached. This class relies on the
 * {@link CompletableFuture} for the concurrency logic. However, it does not accept {@link Throwable} as
 * an exceptional result. This allows attaching listeners that only handle {@link Exception}.
 *
 * @param <T> the result type
 *
 * @opensearch.api
 */
public class CompletableContext<T> {

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();
    private final Set<BiConsumer<T, ? super Exception>> removableListeners = ConcurrentHashMap.newKeySet();

    private volatile T result;
    private volatile Exception failure;
    private volatile boolean completed;

    public CompletableContext() {
        completableFuture.whenComplete((v, t) -> {
            result = v;
            failure = (Exception) t;
            completed = true;
            notifyRemovableListeners();
        });
    }

    public void addListener(BiConsumer<T, ? super Exception> listener) {
        BiConsumer<T, Throwable> castThrowable = (v, t) -> {
            if (t == null) {
                listener.accept(v, null);
            } else {
                assert !(t instanceof Error) : "Cannot be error";
                listener.accept(v, (Exception) t);
            }
        };
        completableFuture.whenComplete(castThrowable);
    }

    public boolean isDone() {
        return completableFuture.isDone();
    }

    public boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    public boolean completeExceptionally(Exception ex) {
        return completableFuture.completeExceptionally(ex);
    }

    public boolean complete(T value) {
        return completableFuture.complete(value);
    }

    /**
     * Adds a listener that can be removed again with {@link #removeRemovableListener(BiConsumer)} while this context
     * is not completed yet. Unlike {@link #addListener(BiConsumer)}, the listener is held as it is given instead of
     * being attached to the underlying {@link CompletableFuture}, whose callbacks cannot be detached. A listener
     * added after this context has completed is notified by the calling thread, as {@link #addListener(BiConsumer)}
     * does. The listeners are held in a set, so adding the same listener instance more than once holds it once.
     *
     * @param listener listener to add
     */
    public void addRemovableListener(BiConsumer<T, ? super Exception> listener) {
        removableListeners.add(listener);
        if (completed) {
            notifyRemovableListeners();
        }
    }

    /**
     * Removes a listener added with {@link #addRemovableListener(BiConsumer)} that has not been notified yet.
     * Removing a listener that was never added, or that has already been notified, does nothing.
     *
     * @param listener listener to remove
     */
    public void removeRemovableListener(BiConsumer<T, ? super Exception> listener) {
        removableListeners.remove(listener);
    }

    /**
     * Number of removable listeners that are waiting to be notified.
     *
     * @return number of listeners
     */
    public int removableListeners() {
        return removableListeners.size();
    }

    private void notifyRemovableListeners() {
        for (BiConsumer<T, ? super Exception> listener : removableListeners) {
            // whoever takes the listener out of the set owns notifying it, so a listener that is added or removed
            // while this context is completing is notified exactly once, or not at all once it has been removed
            if (removableListeners.remove(listener)) {
                listener.accept(result, failure);
            }
        }
    }
}
