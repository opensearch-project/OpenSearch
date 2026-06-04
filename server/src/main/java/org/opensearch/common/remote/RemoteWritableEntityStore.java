/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;

/**
 * An interface to read/write an object from/to a remote storage. This interface is agnostic of the remote storage type.
 *
 * @param <T> The object type which can be uploaded to or downloaded from remote storage.
 * @param <U> The wrapper entity which provides methods for serializing/deserializing entity T.
 */
@ExperimentalApi
public interface RemoteWritableEntityStore<T, U extends RemoteWriteableEntity<T>> {

    public void writeAsync(U entity, ActionListener<Void> listener);

    public T read(U entity) throws IOException;

    public void readAsync(U entity, ActionListener<T> listener);
}
