/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except
 * in compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.opensearch.encryption.frame;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.List;
import java.util.Map;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.internal.EncryptionContextSerializer;
import com.amazonaws.encryptionsdk.internal.TrailingSignatureAlgorithm;
import com.amazonaws.encryptionsdk.model.CiphertextHeaders;
import com.amazonaws.encryptionsdk.model.CiphertextType;
import com.amazonaws.encryptionsdk.model.ContentType;
import com.amazonaws.encryptionsdk.model.EncryptionMaterials;
import com.amazonaws.encryptionsdk.model.KeyBlob;

@SuppressWarnings({ "rawtypes" })
public class EncryptionMetadata {
    private static final CiphertextType CIPHERTEXT_TYPE = CiphertextType.CUSTOMER_AUTHENTICATED_ENCRYPTED_DATA;

    private final Map<String, String> encryptionContext_;
    private final CryptoAlgorithm cryptoAlgo;
    private final List<MasterKey> masterKeys;
    private final List<KeyBlob> keyBlobs;
    private final SecretKey encryptionKey;
    private final byte version;
    private final CiphertextType type;
    private final byte nonceLen;

    private final CiphertextHeaders ciphertextHeaders;
    private final byte[] ciphertextHeaderBytes;
    private final byte[] messageId;
    private final int frameSize;
    private final PrivateKey trailingSignaturePrivateKey;
    private final MessageDigest trailingDigest;
    private final Signature trailingSig;
    private final ContentType contentType;

    public EncryptionMetadata(int frameSize, EncryptionMaterials result) throws AwsCryptoException {
        Utils.assertNonNull(result, "result");

        this.encryptionContext_ = result.getEncryptionContext();

        this.cryptoAlgo = result.getAlgorithm();
        this.masterKeys = result.getMasterKeys();
        this.keyBlobs = result.getEncryptedDataKeys();
        this.trailingSignaturePrivateKey = result.getTrailingSignatureKey();

        if (keyBlobs.isEmpty()) {
            throw new IllegalArgumentException("No encrypted data keys in materials result");
        }

        if (trailingSignaturePrivateKey != null) {
            try {
                TrailingSignatureAlgorithm algorithm = TrailingSignatureAlgorithm.forCryptoAlgorithm(cryptoAlgo);
                trailingDigest = MessageDigest.getInstance(algorithm.getMessageDigestAlgorithm());
                trailingSig = Signature.getInstance(algorithm.getRawSignatureAlgorithm());

                trailingSig.initSign(trailingSignaturePrivateKey, com.amazonaws.encryptionsdk.internal.Utils.getSecureRandom());
            } catch (final GeneralSecurityException ex) {
                throw new AwsCryptoException(ex);
            }
        } else {
            trailingDigest = null;
            trailingSig = null;
        }

        // set default values
        version = cryptoAlgo.getMessageFormatVersion();

        // only allow to encrypt with version 1 crypto algorithms
        if (version != 1) {
            throw new AwsCryptoException(
                "Configuration conflict. Cannot encrypt due to CommitmentPolicy "
                    + CommitmentPolicy.ForbidEncryptAllowDecrypt
                    + " requiring only non-committed messages. Algorithm ID was "
                    + cryptoAlgo
                    + ". See: https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/troubleshooting-migration.html"
            );
        }

        type = CIPHERTEXT_TYPE;
        nonceLen = cryptoAlgo.getNonceLen();

        if (frameSize > 0) {
            contentType = ContentType.FRAME;
        } else if (frameSize == 0) {
            contentType = ContentType.SINGLEBLOCK;
        } else {
            throw Utils.cannotBeNegative("Frame size");
        }

        final CiphertextHeaders unsignedHeaders = createCiphertextHeaders(contentType, frameSize);
        try {
            encryptionKey = cryptoAlgo.getEncryptionKeyFromDataKey(result.getCleartextDataKey(), unsignedHeaders);
        } catch (final InvalidKeyException ex) {
            throw new AwsCryptoException(ex);
        }
        ciphertextHeaders = signCiphertextHeaders(unsignedHeaders);
        ciphertextHeaderBytes = ciphertextHeaders.toByteArray();
        messageId = ciphertextHeaders.getMessageId();
        this.frameSize = frameSize;
    }

    public ContentType getContentType() {
        return contentType;
    }

    /**
     * Create ciphertext headers using the instance variables, and the provided content type and
     * frame size.
     *
     * @param contentType
     *            the content type to set in the ciphertext headers.
     * @param frameSize
     *            the frame size to set in the ciphertext headers.
     * @return the bytes containing the ciphertext headers.
     */
    private CiphertextHeaders createCiphertextHeaders(final ContentType contentType, final int frameSize) {
        // create the ciphertext headers
        final byte[] headerNonce = new byte[nonceLen];
        // We use a deterministic IV of zero for the header authentication.

        final byte[] encryptionContextBytes = EncryptionContextSerializer.serialize(encryptionContext_);
        final CiphertextHeaders ciphertextHeaders = new CiphertextHeaders(
            type,
            cryptoAlgo,
            encryptionContextBytes,
            keyBlobs,
            contentType,
            frameSize
        );
        ciphertextHeaders.setHeaderNonce(headerNonce);

        return ciphertextHeaders;
    }

    private CiphertextHeaders signCiphertextHeaders(final CiphertextHeaders unsignedHeaders) {
        final byte[] headerFields = unsignedHeaders.serializeAuthenticatedFields();
        final byte[] headerTag = computeHeaderTag(unsignedHeaders.getHeaderNonce(), headerFields);

        unsignedHeaders.setHeaderTag(headerTag);

        return unsignedHeaders;
    }

    /**
     * Compute the MAC tag of the header bytes using the provided key, nonce, AAD, and crypto
     * algorithm identifier.
     *
     * @param nonce
     *            the nonce to use in computing the MAC tag.
     * @param aad
     *            the AAD to use in computing the MAC tag.
     * @return the bytes containing the computed MAC tag.
     */
    private byte[] computeHeaderTag(final byte[] nonce, final byte[] aad) {
        final CipherHandler cipherHandler = new CipherHandler(encryptionKey, Cipher.ENCRYPT_MODE, cryptoAlgo);

        return cipherHandler.cipherData(nonce, aad, new byte[0], 0, 0);
    }

    public Map<String, String> getEncryptionContext() {
        return encryptionContext_;
    }

    public CryptoAlgorithm getCryptoAlgo() {
        return cryptoAlgo;
    }

    public List<MasterKey> getMasterKeys() {
        return masterKeys;
    }

    public List<KeyBlob> getKeyBlobs() {
        return keyBlobs;
    }

    public SecretKey getEncryptionKey() {
        return encryptionKey;
    }

    public byte getVersion() {
        return version;
    }

    public CiphertextType getType() {
        return type;
    }

    public byte getNonceLen() {
        return nonceLen;
    }

    public CiphertextHeaders getCiphertextHeaders() {
        return ciphertextHeaders;
    }

    public byte[] getCiphertextHeaderBytes() {
        return ciphertextHeaderBytes;
    }

    public byte[] getMessageId() {
        return messageId;
    }

    public int getFrameSize() {
        return frameSize;
    }

    public PrivateKey getTrailingSignaturePrivateKey() {
        return trailingSignaturePrivateKey;
    }

    public MessageDigest getTrailingDigest() {
        return trailingDigest;
    }

    public Signature getTrailingSig() {
        return trailingSig;
    }
}
