/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import java.io.IOException;
import java.io.InputStream;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;

/**
 * An interface to read/write and object from/to a remote storage. This interface is agnostic of the remote storage type.
 *
 * @param <T> The object type which can be upload to or download from remote storage.
 */
public interface RemoteObject<T> {

    /**
     * @return The entity T contained within this class
     */
    public T get();

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
