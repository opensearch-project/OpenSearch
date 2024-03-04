/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.keystore;

/**
 * An interface for objects that hold an in-memory record of hashes of keys in the disk cache.
 * These objects have some internal data structure which stores some transformation of added
 * int values. The internal representations may have collisions. Example transformations include a modulo
 * or -abs(value), or some combination.
 */
public interface KeyLookupStore<T> {

    /**
     * Transforms the input value into the internal representation for this keystore
     * and adds it to the internal data structure.
     * @param value The value to add.
     * @return true if the value was added, false if it wasn't added because of a
     * collision or if it was already present.
     */
    boolean add(T value);

    /**
     * Checks if the transformation of the value is in the keystore.
     * @param value The value to check.
     * @return true if the value was found, false otherwise. Due to collisions, false positives are
     * possible, but there should be no false negatives unless forceRemove() is called.
     */
    boolean contains(T value);

    /**
     * Attempts to safely remove a value from the internal structure, maintaining the property that contains(value)
     * will never return a false negative. If removing would lead to a false negative, the value won't be removed.
     * Classes may not implement safe removal.
     * @param value The value to attempt to remove.
     * @return true if the value was removed, false if it wasn't.
     */
    boolean remove(T value);

    /**
     * Returns the number of distinct values stored in the internal data structure.
     * Does not count values which weren't successfully added due to collisions.
     * @return The number of values
     */
    int getSize();

    /**
     * Returns an estimate of the store's memory usage.
     * @return The memory usage
     */
    long getMemorySizeInBytes();

    /**
     * Returns the cap for the store's memory usage.
     * @return The cap, in bytes
     */
    long getMemorySizeCapInBytes();

    /**
     * Returns whether the store is at memory capacity and can't accept more entries
     */
    boolean isFull();

    /**
     * Deletes the internal data structure and regenerates it from the values passed in.
     * Also resets all stats related to adding.
     * @param newValues The keys that should be in the reset structure.
     */
    void regenerateStore(T[] newValues);

    /**
     * Deletes all keys and resets all stats related to adding.
     */
    void clear();
}
