/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;

import java.io.IOException;

/**
 * Adjusts length of encrypted blob to raw length
 */
public class EncryptedBlobMetadata implements BlobMetadata {
    private final EncryptedHeaderContentSupplier encryptedHeaderContentSupplier;
    private final BlobMetadata delegate;
    private final CryptoProvider cryptoProvider;

    public EncryptedBlobMetadata(
        BlobMetadata delegate,
        CryptoProvider cryptoProvider,
        EncryptedHeaderContentSupplier encryptedHeaderContentSupplier
    ) {
        this.encryptedHeaderContentSupplier = encryptedHeaderContentSupplier;
        this.delegate = delegate;
        this.cryptoProvider = cryptoProvider;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public long length() {
        Object cryptoContext;
        try {
            cryptoContext = cryptoProvider.loadEncryptionMetadata(encryptedHeaderContentSupplier);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return cryptoProvider.estimateDecryptedLength(cryptoContext, delegate.length());
    }
}
