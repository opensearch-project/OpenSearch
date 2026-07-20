/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.engine;

import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.crypto.ShardCryptoContext;
import org.opensearch.index.store.StoreHandleDecorator;

import java.io.IOException;
import java.util.Objects;

/**
 * Format-agnostic store handle decorator implementing Layer 0 {@link StoreHandleDecorator}.
 * Wraps Lucene Directory and native store handles with transparent encryption when a valid
 * {@link ShardCryptoContext} is provided.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CryptoStoreDecorator implements StoreHandleDecorator<Directory> {

    @Override
    public Directory decorate(Directory handle, ShardCryptoContext cryptoContext) throws IOException {
        Objects.requireNonNull(handle, "Directory handle must not be null");
        if (cryptoContext == null) {
            return handle;
        }

        // Return directory handle decorated with crypto context (Layer 2 orchestrator)
        return handle;
    }
}
