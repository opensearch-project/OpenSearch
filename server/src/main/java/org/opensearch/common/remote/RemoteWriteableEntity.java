/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface to which provides defines the serialization/deserialization methods for objects to be uploaded to or downloaded from remote store.
 * This interface is agnostic of the remote storage type.
 *
 * @param <T> The object type which can be uploaded to or downloaded from remote storage.
 */
public interface RemoteWriteableEntity<T> {
    /**
     * @return An InputStream created by serializing the entity T
     * @throws IOException Exception encountered while serialization
     */
    public InputStream serialize() throws IOException;

    /**
     * @param inputStream The InputStream which is used to read the serialized entity
     * @return The entity T after deserialization
     * @throws IOException Exception encountered while deserialization
     */
    public T deserialize(InputStream inputStream) throws IOException;

}
