/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.core.action.ActionListener;

import java.io.IOException;

/**
 * An interface to read/write an object from/to a remote storage. This interface is agnostic of the remote storage type.
 *
 * @param <T> The object type which can be uploaded to or downloaded from remote storage.
 */
public interface RemoteObjectStore<T, U extends RemoteObject<T>> {

    public void writeAsync(RemoteObject<T> obj, ActionListener<Void> listener);

    public T read(RemoteObject<T> obj) throws IOException;

    public void readAsync(RemoteObject<T> obj, ActionListener<T> listener);
}
