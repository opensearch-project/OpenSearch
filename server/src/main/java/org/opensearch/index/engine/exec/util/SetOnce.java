/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.util;

import java.util.concurrent.atomic.AtomicReference;

public final class SetOnce<T> implements Cloneable {

    /** Thrown when {@link SetOnce#set(Object)} is called more than once. */
    public static final class AlreadySetException extends IllegalStateException {
        public AlreadySetException() {
            super("The object cannot be set twice!");
        }
    }

    /** Holding object and marking that it was already set */
    private static final class Wrapper<T> {
        private T object;

        private Wrapper(T object) {
            this.object = object;
        }
    }

    private final AtomicReference<Wrapper<T>> set;

    /**
     * A default constructor which does not set the internal object, and allows setting it by calling
     * {@link #set(Object)}.
     */
    public SetOnce() {
        set = new AtomicReference<>();
    }

    /**
     * Creates a new instance with the internal object set to the given object. Note that any calls to
     * {@link #set(Object)} afterwards will result in {@link AlreadySetException}
     *
     * @throws AlreadySetException if called more than once
     * @see #set(Object)
     */
    public SetOnce(T obj) {
        set = new AtomicReference<>(new Wrapper<>(obj));
    }

    /** Sets the given object. If the object has already been set, an exception is thrown. */
    public final void set(T obj) {
        if (!trySet(obj)) {
            throw new AlreadySetException();
        }
    }

    /**
     * Sets the given object if none was set before.
     *
     * @return true if object was set successfully, false otherwise
     */
    public final boolean trySet(T obj) {
        return set.compareAndSet(null, new Wrapper<>(obj));
    }

    /** Returns the object set by {@link #set(Object)}. */
    public final T get() {
        Wrapper<T> wrapper = set.get();
        return wrapper == null ? null : wrapper.object;
    }
}
