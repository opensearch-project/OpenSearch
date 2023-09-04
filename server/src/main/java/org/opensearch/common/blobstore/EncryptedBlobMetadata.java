/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.crypto.CryptoHandler;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;

import java.io.IOException;

/**
 * Adjusts length of encrypted blob to raw length
 */
public class EncryptedBlobMetadata<T, U> implements BlobMetadata {
    private final EncryptedHeaderContentSupplier encryptedHeaderContentSupplier;
    private final BlobMetadata delegate;
    private final CryptoHandler<T, U> cryptoHandler;

    public EncryptedBlobMetadata(
        BlobMetadata delegate,
        CryptoHandler<T, U> cryptoHandler,
        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier
    ) {
        this.encryptedHeaderContentSupplier = encryptedHeaderContentSupplier;
        this.delegate = delegate;
        this.cryptoHandler = cryptoHandler;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public long length() {
        U cryptoContext;
        try {
            cryptoContext = cryptoHandler.loadEncryptionMetadata(encryptedHeaderContentSupplier);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return cryptoHandler.estimateDecryptedLength(cryptoContext, delegate.length());
    }
}
