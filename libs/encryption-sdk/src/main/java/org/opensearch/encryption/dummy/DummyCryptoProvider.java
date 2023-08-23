/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption.dummy;

import com.amazonaws.encryptionsdk.caching.CachingCryptoMaterialsManager;
import org.opensearch.common.crypto.CryptoProvider;
import org.opensearch.common.crypto.DecryptedRangedStreamProvider;
import org.opensearch.common.crypto.EncryptedHeaderContentSupplier;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;
import java.io.InputStream;

public class DummyCryptoProvider implements CryptoProvider {
    CachingCryptoMaterialsManager materialsManager;
    MasterKeyProvider masterKeyProvider;

    public DummyCryptoProvider(CachingCryptoMaterialsManager materialsManager, MasterKeyProvider masterKeyProvider) {
        this.materialsManager = materialsManager;
        this.masterKeyProvider = masterKeyProvider;
    }

    @Override
    public Object initEncryptionMetadata() {
        return new Object();
    }

    @Override
    public long adjustContentSizeForPartialEncryption(Object cryptoContextObj, long contentSize) {
        return contentSize;
    }

    @Override
    public long estimateEncryptedLengthOfEntireContent(Object cryptoContextObj, long contentLength) {
        return contentLength;
    }

    @Override
    public InputStreamContainer createEncryptingStream(Object encryptionMetadata, InputStreamContainer streamContainer) {
        return streamContainer;
    }

    @Override
    public InputStreamContainer createEncryptingStreamOfPart(
        Object cryptoContextObj,
        InputStreamContainer stream,
        int totalStreams,
        int streamIdx
    ) {
        return stream;
    }

    @Override
    public InputStream createDecryptingStream(InputStream encryptingStream) {
        return encryptingStream;
    }

    @Override
    public Object loadEncryptionMetadata(EncryptedHeaderContentSupplier encryptedHeaderContentSupplier) throws IOException {
        return null;
    }

    @Override
    public DecryptedRangedStreamProvider createDecryptingStreamOfRange(
        Object cryptoContext,
        long startPosOfRawContent,
        long endPosOfRawContent
    ) {
        return null;
    }

    @Override
    public long estimateDecryptedLength(Object cryptoContext, long contentLength) {
        return contentLength;
    }
}
