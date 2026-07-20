/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.ShardCryptoContext;

import java.io.IOException;

/**
 * SPI contract for decorating storage handles (e.g., Lucene Directory, ObjectStore handles)
 * with transparent encryption and decryption layers without format-specific hard coupling
 * (Layer 0 contract).
 *
 * @param <T> the storage handle type (e.g., Directory, ObjectStore)
 * @opensearch.experimental
 */
@ExperimentalApi
public interface StoreHandleDecorator<T> {

    /**
     * Decorates an unencrypted storage handle with encryption/decryption capabilities
     * for a specific shard context.
     *
     * @param handle the underlying unencrypted storage handle
     * @param cryptoContext the shard encryption context containing key material and metadata
     * @return the decorated, transparently encrypting/decrypting storage handle
     * @throws IOException if decorating the handle fails
     */
    T decorate(T handle, ShardCryptoContext cryptoContext) throws IOException;
}
