/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.serializer;

/**
 * An interface for serializers, to be used in disk caching tier and elsewhere.
 * T is the class of the original object, and U is the serialized class.
 */
public interface Serializer<T, U> {
    /**
     * Serializes an object.
     * @param object A non-serialized object.
     * @return The serialized representation of the object.
     */
    U serialize(T object);

    /**
     * Deserializes bytes into an object.
     * @param bytes The serialized representation.
     * @return The original object.
     */
    T deserialize(U bytes);

    /**
     * Compares an object to a serialized representation of an object.
     * @param object A non-serialized objet
     * @param bytes Serialized representation of an object
     * @return true if representing the same object, false if not
     */
    boolean equals(T object, U bytes);
}
