/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.crypto.CryptoManager;

import java.io.IOException;

/**
 * Adjusts length of encrypted blob to raw length
 */
public class EncryptedBlobMetadata implements BlobMetadata {
    private final EncryptedHeaderContentSupplier encryptedHeaderContentSupplier;
    private final BlobMetadata delegate;
    private final CryptoManager cryptoManager;

    public EncryptedBlobMetadata(BlobMetadata delegate, CryptoManager cryptoManager,
                                 EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) {
        this.encryptedHeaderContentSupplier = encryptedHeaderContentSupplier;
        this.delegate = delegate;
        this.cryptoManager = cryptoManager;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public long length() {
        Object cryptoContext;
        try {
            cryptoContext = cryptoManager.loadEncryptionMetadata(encryptedHeaderContentSupplier);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        return cryptoManager.estimateDecryptedLength(cryptoContext, delegate.length());
    }
}
