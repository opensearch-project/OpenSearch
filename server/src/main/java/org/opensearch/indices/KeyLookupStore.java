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

package org.opensearch.indices;

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
    boolean add(T value) throws Exception;

    /**
     * Checks if the transformation of the value is in the keystore.
     * @param value The value to check.
     * @return true if the value was found, false otherwise. Due to collisions, false positives are
     * possible, but there should be no false negatives unless forceRemove() is called.
     */
    boolean contains(T value) throws Exception;

    /**
     * Returns the transformed version of the input value, that would be used to stored it in the keystore.
     * This transformation should be always be the same for a given instance.
     * @param value The value to transform.
     * @return The transformed value.
     */
    T getInternalRepresentation(T value);

    /**
     * Attempts to safely remove a value from the internal structure, maintaining the property that contains(value)
     * will never return a false negative. If removing would lead to a false negative, the value won't be removed.
     * Classes may not implement safe removal.
     * @param value The value to attempt to remove.
     * @return true if the value was removed, false if it wasn't.
     */
    boolean remove(T value) throws Exception;

    /**
     * Returns the number of distinct values stored in the internal data structure.
     * Does not count values which weren't successfully added due to collisions.
     * @return The number of values
     */
    int getSize();

    /**
     * Returns the number of times add() has been run, including unsuccessful attempts.
     * @return The number of adding attempts.
     */
    int getTotalAdds();

    /**
     * Returns the number of times add() has returned false due to a collision.
     * @return The number of collisions.
     */
    int getCollisions();


    /**
     * Checks if two values would collide after being transformed by this store's transformation.
     * @param value1 The first value to compare.
     * @param value2 The second value to compare.
     * @return true if the transformations are equal, false otherwise.
     */
    boolean isCollision(T value1, T value2);

    /**
     * Returns an estimate of the store's memory usage.
     * @return The memory usage, in MB
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
    void regenerateStore(T[] newValues) throws Exception;

    /**
     * Deletes all keys and resets all stats related to adding.
     */
    void clear() throws Exception;
}
