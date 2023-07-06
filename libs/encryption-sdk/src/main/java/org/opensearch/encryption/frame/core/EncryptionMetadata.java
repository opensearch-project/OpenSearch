/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame.core;

import com.amazonaws.encryptionsdk.CommitmentPolicy;
import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.internal.CommittedKey;
import com.amazonaws.encryptionsdk.internal.EncryptionContextSerializer;
import com.amazonaws.encryptionsdk.internal.TrailingSignatureAlgorithm;
import com.amazonaws.encryptionsdk.model.CiphertextHeaders;
import com.amazonaws.encryptionsdk.model.CiphertextType;
import com.amazonaws.encryptionsdk.model.ContentType;
import com.amazonaws.encryptionsdk.model.EncryptionMaterials;
import com.amazonaws.encryptionsdk.model.KeyBlob;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
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

    public EncryptionMetadata(int frameSize, EncryptionMaterials result, CommitmentPolicy commitmentPolicy) throws AwsCryptoException {
        Utils.assertNonNull(result, "result");
        Utils.assertNonNull(commitmentPolicy, "commitmentPolicy");

        this.encryptionContext_ = result.getEncryptionContext();
        if (!commitmentPolicy.algorithmAllowedForEncrypt(result.getAlgorithm())) {
            if (commitmentPolicy == CommitmentPolicy.ForbidEncryptAllowDecrypt) {
                throw new AwsCryptoException(
                    "Configuration conflict. Cannot encrypt due to CommitmentPolicy "
                        + commitmentPolicy
                        + " requiring only non-committed messages. Algorithm ID was "
                        + result.getAlgorithm()
                );
            } else {
                throw new AwsCryptoException(
                    "Configuration conflict. Cannot encrypt due to CommitmentPolicy "
                        + commitmentPolicy
                        + " requiring only committed messages. Algorithm ID was "
                        + result.getAlgorithm()
                );
            }
        }

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

                trailingSig.initSign(trailingSignaturePrivateKey, Utils.getSecureRandom());
            } catch (final GeneralSecurityException ex) {
                throw new AwsCryptoException(ex);
            }
        } else {
            trailingDigest = null;
            trailingSig = null;
        }

        // set default values
        version = cryptoAlgo.getMessageFormatVersion();
        type = CIPHERTEXT_TYPE;
        nonceLen = cryptoAlgo.getNonceLen();

        ContentType contentType;
        if (frameSize > 0) {
            contentType = ContentType.FRAME;
        } else if (frameSize == 0) {
            contentType = ContentType.SINGLEBLOCK;
        } else {
            throw Utils.cannotBeNegative("Frame size");
        }

        // Construct the headers
        // Included here rather than as a sub-routine so we can set final variables.
        // This way we can avoid calculating the keys more times than we need.
        final byte[] encryptionContextBytes = EncryptionContextSerializer.serialize(encryptionContext_);
        final CiphertextHeaders unsignedHeaders = new CiphertextHeaders(
            type,
            cryptoAlgo,
            encryptionContextBytes,
            keyBlobs,
            contentType,
            frameSize
        );
        // We use a deterministic IV of zero for the header authentication.
        unsignedHeaders.setHeaderNonce(new byte[nonceLen]);

        // If using a committing crypto algorithm, we also need to calculate the commitment value along
        // with the key derivation
        if (cryptoAlgo.isCommitting()) {
            final CommittedKey committedKey = CommittedKey.generate(
                cryptoAlgo,
                result.getCleartextDataKey(),
                unsignedHeaders.getMessageId()
            );
            unsignedHeaders.setSuiteData(committedKey.getCommitment());
            encryptionKey = committedKey.getKey();
        } else {
            try {
                encryptionKey = cryptoAlgo.getEncryptionKeyFromDataKey(result.getCleartextDataKey(), unsignedHeaders);
            } catch (final InvalidKeyException ex) {
                throw new AwsCryptoException(ex);
            }
        }

        ciphertextHeaders = signCiphertextHeaders(unsignedHeaders);
        ciphertextHeaderBytes = ciphertextHeaders.toByteArray();
        messageId = ciphertextHeaders.getMessageId();
        this.frameSize = frameSize;
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
     * @param nonce the nonce to use in computing the MAC tag.
     * @param aad the AAD to use in computing the MAC tag.
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
