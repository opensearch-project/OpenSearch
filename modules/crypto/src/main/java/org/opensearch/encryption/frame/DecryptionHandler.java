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
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.amazonaws.encryptionsdk.CryptoAlgorithm;
import com.amazonaws.encryptionsdk.CryptoMaterialsManager;
import com.amazonaws.encryptionsdk.DataKey;
import com.amazonaws.encryptionsdk.DefaultCryptoMaterialsManager;
import com.amazonaws.encryptionsdk.MasterKey;
import com.amazonaws.encryptionsdk.MasterKeyProvider;
import com.amazonaws.encryptionsdk.exception.AwsCryptoException;
import com.amazonaws.encryptionsdk.exception.BadCiphertextException;
import com.amazonaws.encryptionsdk.internal.CryptoHandler;
import com.amazonaws.encryptionsdk.internal.EncryptionHandler;
import com.amazonaws.encryptionsdk.internal.MessageCryptoHandler;
import com.amazonaws.encryptionsdk.internal.ProcessingSummary;
import com.amazonaws.encryptionsdk.internal.TrailingSignatureAlgorithm;
import com.amazonaws.encryptionsdk.model.CiphertextFooters;
import com.amazonaws.encryptionsdk.model.CiphertextHeaders;
import com.amazonaws.encryptionsdk.model.CiphertextType;
import com.amazonaws.encryptionsdk.model.ContentType;
import com.amazonaws.encryptionsdk.model.DecryptionMaterials;
import com.amazonaws.encryptionsdk.model.DecryptionMaterialsRequest;

/**
 * This class implements the CryptoHandler interface by providing methods for
 * the decryption of ciphertext produced by the methods in
 * {@link EncryptionHandler}.
 *
 * <p>
 * This class reads and parses the values in the ciphertext headers and
 * delegates the decryption of the ciphertext to the
 * content type parsed in the ciphertext headers.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class DecryptionHandler<K extends MasterKey<K>> implements MessageCryptoHandler {
    private final CryptoMaterialsManager materialsManager_;

    private final CiphertextHeaders ciphertextHeaders_;
    private final CiphertextFooters ciphertextFooters_;
    private boolean ciphertextHeadersParsed_;

    private CryptoHandler contentCryptoHandler_;

    private DataKey<K> dataKey_;
    private SecretKey decryptionKey_;
    private CryptoAlgorithm cryptoAlgo_;
    private Signature trailingSig_;

    private Map<String, String> encryptionContext_ = null;

    private byte[] unparsedBytes_ = new byte[0];
    private boolean complete_ = false;

    private long ciphertextSizeBound_ = -1;
    private long ciphertextBytesSupplied_ = 0;

    // These ctors are private to ensure type safety - we must ensure construction using a CMM results in a
    // DecryptionHandler<?>, not a DecryptionHandler<SomeConcreteType>, since the CryptoMaterialsManager is not itself
    // genericized.
    private DecryptionHandler(final CryptoMaterialsManager materialsManager) {
        com.amazonaws.encryptionsdk.internal.Utils.assertNonNull(materialsManager, "materialsManager");

        this.materialsManager_ = materialsManager;
        ciphertextHeaders_ = new CiphertextHeaders();
        ciphertextFooters_ = new CiphertextFooters();
    }

    private DecryptionHandler(final CryptoMaterialsManager materialsManager, final CiphertextHeaders headers, final int frameStartNum)
        throws AwsCryptoException {
        com.amazonaws.encryptionsdk.internal.Utils.assertNonNull(materialsManager, "materialsManager");

        materialsManager_ = materialsManager;
        ciphertextHeaders_ = headers;
        ciphertextFooters_ = new CiphertextFooters();
        readHeaderFields(headers, frameStartNum);
        updateTrailingSignature(headers);
    }

    /**
     * Create a decryption handler using the provided master key.
     *
     * <p>
     * Note the methods in the provided master key are used in decrypting the
     * encrypted data key parsed from the ciphertext headers.
     *
     * @param customerMasterKeyProvider
     *            the master key provider to use in picking a master key from
     *            the key blobs encoded in the provided ciphertext.
     * @throws AwsCryptoException
     *             if the master key is null.
     */
    @SuppressWarnings("unchecked")
    public static <K extends MasterKey<K>> DecryptionHandler<K> create(final MasterKeyProvider<K> customerMasterKeyProvider)
        throws AwsCryptoException {
        Utils.assertNonNull(customerMasterKeyProvider, "customerMasterKeyProvider");

        return (DecryptionHandler<K>) create(new DefaultCryptoMaterialsManager(customerMasterKeyProvider));
    }

    /**
     * Create a decryption handler using the provided materials manager.
     *
     * <p>
     * Note the methods in the provided materials manager are used in decrypting the encrypted data key
     * parsed from the ciphertext headers.
     *
     * @param materialsManager
     *            the materials manager to use in decrypting the data key from the key blobs encoded
     *            in the provided ciphertext.
     * @throws AwsCryptoException
     *             if the master key is null.
     */
    public static DecryptionHandler<?> create(final CryptoMaterialsManager materialsManager) throws AwsCryptoException {
        return new DecryptionHandler(materialsManager);
    }

    /**
     * Create a decryption handler using the provided materials manager and already parsed {@code headers}.
     *
     * <p>
     * Note the methods in the provided materials manager are used in decrypting the encrypted data key
     * parsed from the ciphertext headers.
     *
     * @param materialsManager the materials manager to use in decrypting the data key from the key
     *     blobs encoded in the provided ciphertext.
     * @param headers already parsed headers which will not be passed into {@link
     *     #processBytes(byte[], int, int, byte[], int)}
     *     decryption; zero indicates no maximum
     * @throws AwsCryptoException if the master key is null.
     * @param frameStartNum Number from which assignment has to start for new frames
     * @return instance of {@link DecryptionHandler}     *
     */
    public static DecryptionHandler<?> create(
        final CryptoMaterialsManager materialsManager,
        final CiphertextHeaders headers,
        final int frameStartNum
    ) throws AwsCryptoException {
        return new DecryptionHandler(materialsManager, headers, frameStartNum);
    }

    /**
     * Decrypt the ciphertext bytes provided in {@code in} and copy the plaintext bytes to
     * {@code out}.
     *
     * <p>
     * This method consumes and parses the ciphertext headers. The decryption of the actual content
     * is delegated to {@link FrameDecryptionHandler} based on the
     * content type parsed in the ciphertext header.
     *
     * @param in
     *            the input byte array.
     * @param off
     *            the offset into the in array where the data to be decrypted starts.
     * @param len
     *            the number of bytes to be decrypted.
     * @param out
     *            the output buffer the decrypted plaintext bytes go into.
     * @param outOff
     *            the offset into the output byte array the decrypted data starts at.
     * @return the number of bytes written to {@code out} and processed.
     *
     * @throws BadCiphertextException
     *             if the ciphertext header contains invalid entries or if the header integrity
     *             check fails.
     * @throws AwsCryptoException
     *             if any of the offset or length arguments are negative or if the total bytes to
     *             decrypt exceeds the maximum allowed value.
     */
    @Override
    public ProcessingSummary processBytes(final byte[] in, final int off, final int len, final byte[] out, final int outOff)
        throws BadCiphertextException, AwsCryptoException {

        if (len < 0 || off < 0) {
            throw new AwsCryptoException("Invalid values for input offset: " + off + "and length:" + len);
        }

        if (in.length == 0 || len == 0) {
            return ProcessingSummary.ZERO;
        }

        final long totalBytesToParse = unparsedBytes_.length + (long) len;
        // check for integer overflow
        if (totalBytesToParse > Integer.MAX_VALUE) {
            throw new AwsCryptoException("Size of the total bytes to parse and decrypt exceeded allowed maximum:" + Integer.MAX_VALUE);
        }

        checkSizeBound(len);
        ciphertextBytesSupplied_ += len;

        final byte[] bytesToParse = new byte[(int) totalBytesToParse];
        final int leftoverBytes = unparsedBytes_.length;
        // If there were previously unparsed bytes, add them as the first
        // set of bytes to be parsed in this call.
        System.arraycopy(unparsedBytes_, 0, bytesToParse, 0, unparsedBytes_.length);
        System.arraycopy(in, off, bytesToParse, unparsedBytes_.length, len);

        int totalParsedBytes = 0;
        if (ciphertextHeadersParsed_ == false) {
            totalParsedBytes += ciphertextHeaders_.deserialize(bytesToParse, 0);
            // When ciphertext headers are complete, we have the data
            // key and cipher mode to initialize the underlying cipher
            if (ciphertextHeaders_.isComplete() == true) {
                readHeaderFields(ciphertextHeaders_, 1);
                updateTrailingSignature(ciphertextHeaders_);
                // reset unparsed bytes as parsing of ciphertext headers is
                // complete.
                unparsedBytes_ = new byte[0];
            } else {
                // If there aren't enough bytes to parse ciphertext
                // headers, we don't have anymore bytes to continue parsing.
                // But first copy the leftover bytes to unparsed bytes.
                unparsedBytes_ = Arrays.copyOfRange(bytesToParse, totalParsedBytes, bytesToParse.length);
                return new ProcessingSummary(0, len);
            }
        }

        int actualOutLen = 0;
        if (!contentCryptoHandler_.isComplete()) {
            // if there are bytes to parse further, pass it off to underlying
            // content cryptohandler.
            if ((bytesToParse.length - totalParsedBytes) > 0) {
                final ProcessingSummary contentResult = contentCryptoHandler_.processBytes(
                    bytesToParse,
                    totalParsedBytes,
                    bytesToParse.length - totalParsedBytes,
                    out,
                    outOff
                );
                updateTrailingSignature(bytesToParse, totalParsedBytes, contentResult.getBytesProcessed());
                actualOutLen = contentResult.getBytesWritten();
                totalParsedBytes += contentResult.getBytesProcessed();

            }
            if (contentCryptoHandler_.isComplete()) {
                actualOutLen += contentCryptoHandler_.doFinal(out, outOff + actualOutLen);
            }
        }

        if (contentCryptoHandler_.isComplete()) {
            // If the crypto algorithm contains trailing signature, we will need to verify
            // the footer of the message.
            if (cryptoAlgo_.getTrailingSignatureLength() > 0) {
                totalParsedBytes += ciphertextFooters_.deserialize(bytesToParse, totalParsedBytes);
                if (ciphertextFooters_.isComplete() && trailingSig_ != null) {
                    try {
                        if (!trailingSig_.verify(ciphertextFooters_.getMAuth())) {
                            throw new BadCiphertextException("Bad trailing signature");
                        }
                    } catch (final SignatureException ex) {
                        throw new BadCiphertextException("Bad trailing signature", ex);
                    }
                    complete_ = true;
                }
            } else {
                complete_ = true;
            }
        }
        return new ProcessingSummary(actualOutLen, totalParsedBytes - leftoverBytes);
    }

    /**
     * Finish processing of the bytes.
     *
     * @param out
     *            space for any resulting output data.
     * @param outOff
     *            offset into {@code out} to start copying the data at.
     * @return
     *         number of bytes written into {@code out}.
     * @throws BadCiphertextException
     *             if the bytes do not decrypt correctly.
     */
    @Override
    public int doFinal(final byte[] out, final int outOff) throws BadCiphertextException {
        // check if cryptohandler for content has been created. There are cases
        // when it might not have been created such as when doFinal() is called
        // before the ciphertext headers are fully received and parsed.
        if (contentCryptoHandler_ == null) {
            return 0;
        } else {

            int result = contentCryptoHandler_.doFinal(out, outOff);

            if (!ciphertextHeaders_.isComplete() || !contentCryptoHandler_.isComplete()) {
                throw new BadCiphertextException("Unable to process entire ciphertext.");
            }

            return result;
        }
    }

    /**
     * Return the size of the output buffer required for a
     * <code>processBytes</code> plus a <code>doFinal</code> with an input of
     * inLen bytes.
     *
     * @param inLen
     *            the length of the input.
     * @return
     *         the space required to accommodate a call to processBytes and
     *         doFinal with input of size {@code inLen} bytes.
     */
    @Override
    public int estimateOutputSize(final int inLen) {
        if (contentCryptoHandler_ != null) {
            return contentCryptoHandler_.estimateOutputSize(inLen);
        } else {
            return Math.max(inLen, 0);
        }
    }

    @Override
    public int estimatePartialOutputSize(int inLen) {
        if (contentCryptoHandler_ != null) {
            return contentCryptoHandler_.estimatePartialOutputSize(inLen);
        } else {
            return Math.max(inLen, 0);
        }
    }

    @Override
    public int estimateFinalOutputSize() {
        if (contentCryptoHandler_ != null) {
            return contentCryptoHandler_.estimateFinalOutputSize();
        } else {
            return 0;
        }
    }

    /**
     * Return the encryption context. This value is parsed from the ciphertext.
     *
     * @return
     *         the key-value map containing the encryption client.
     */
    @Override
    public Map<String, String> getEncryptionContext() {
        return encryptionContext_;
    }

    private void checkSizeBound(long additionalBytes) {
        if (ciphertextSizeBound_ != -1 && ciphertextBytesSupplied_ + additionalBytes > ciphertextSizeBound_) {
            throw new IllegalStateException("Ciphertext size exceeds size bound");
        }
    }

    @Override
    public void setMaxInputLength(long size) {
        if (size < 0) {
            throw Utils.cannotBeNegative("Max input length");
        }

        if (ciphertextSizeBound_ != -1 && ciphertextSizeBound_ < size) {
            ciphertextSizeBound_ = size;
        }

        // check that we haven't already exceeded the limit
        checkSizeBound(0);
    }

    /**
     * Check integrity of the header bytes by processing the parsed MAC tag in
     * the headers through the cipher.
     *
     * @param ciphertextHeaders
     *            the ciphertext headers object whose integrity needs to be
     *            checked.
     */
    private void verifyHeaderIntegrity(final CiphertextHeaders ciphertextHeaders) throws BadCiphertextException {
        final CipherHandler cipherHandler = new CipherHandler(decryptionKey_, Cipher.DECRYPT_MODE, cryptoAlgo_);

        try {
            final byte[] headerTag = ciphertextHeaders.getHeaderTag();
            cipherHandler.cipherData(
                ciphertextHeaders.getHeaderNonce(),
                ciphertextHeaders.serializeAuthenticatedFields(),
                headerTag,
                0,
                headerTag.length
            );
        } catch (BadCiphertextException e) {
            throw new BadCiphertextException("Header integrity check failed.", e);
        }
    }

    /**
     * Read the fields in the ciphertext headers to populate the corresponding
     * instance variables used during decryption.
     *
     * @param ciphertextHeaders
     *            the ciphertext headers object to read.
     */
    @SuppressWarnings("unchecked")
    private void readHeaderFields(final CiphertextHeaders ciphertextHeaders, final int frameStartNum) {
        cryptoAlgo_ = ciphertextHeaders.getCryptoAlgoId();

        final CiphertextType ciphertextType = ciphertextHeaders.getType();
        if (ciphertextType != CiphertextType.CUSTOMER_AUTHENTICATED_ENCRYPTED_DATA) {
            throw new BadCiphertextException("Invalid type in ciphertext.");
        }

        final byte[] messageId = ciphertextHeaders.getMessageId();

        encryptionContext_ = ciphertextHeaders.getEncryptionContextMap();

        DecryptionMaterialsRequest request = DecryptionMaterialsRequest.newBuilder()
            .setAlgorithm(cryptoAlgo_)
            .setEncryptionContext(encryptionContext_)
            .setEncryptedDataKeys(ciphertextHeaders.getEncryptedKeyBlobs())
            .build();

        DecryptionMaterials result = materialsManager_.decryptMaterials(request);

        // noinspection unchecked
        dataKey_ = (DataKey<K>) result.getDataKey();
        PublicKey trailingPublicKey = result.getTrailingSignatureKey();

        try {
            decryptionKey_ = cryptoAlgo_.getEncryptionKeyFromDataKey(dataKey_.getKey(), ciphertextHeaders);
        } catch (final InvalidKeyException ex) {
            throw new AwsCryptoException(ex);
        }

        if (cryptoAlgo_.getTrailingSignatureLength() > 0) {
            Utils.assertNonNull(trailingPublicKey, "trailing public key");

            TrailingSignatureAlgorithm trailingSignatureAlgorithm = TrailingSignatureAlgorithm.forCryptoAlgorithm(cryptoAlgo_);

            try {
                trailingSig_ = Signature.getInstance(trailingSignatureAlgorithm.getHashAndSignAlgorithm());

                trailingSig_.initVerify(trailingPublicKey);
            } catch (GeneralSecurityException e) {
                throw new AwsCryptoException(e);
            }
        } else {
            if (trailingPublicKey != null) {
                throw new AwsCryptoException("Unexpected trailing signature key in context");
            }

            trailingSig_ = null;
        }

        final ContentType contentType = ciphertextHeaders.getContentType();

        final short nonceLen = ciphertextHeaders.getNonceLength();
        final int frameLen = ciphertextHeaders.getFrameLength();

        verifyHeaderIntegrity(ciphertextHeaders);

        // should never get here because an invalid content type is
        // detected when parsing.
        if (Objects.requireNonNull(contentType) == ContentType.FRAME) {
            contentCryptoHandler_ = new FrameDecryptionHandler(
                decryptionKey_,
                (byte) nonceLen,
                cryptoAlgo_,
                messageId,
                frameLen,
                frameStartNum
            );
        }

        ciphertextHeadersParsed_ = true;
    }

    private void updateTrailingSignature(final CiphertextHeaders headers) {
        if (trailingSig_ != null) {
            final byte[] reserializedHeaders = headers.toByteArray();
            updateTrailingSignature(reserializedHeaders, 0, reserializedHeaders.length);
        }
    }

    private void updateTrailingSignature(byte[] input, int offset, int len) {
        if (trailingSig_ != null) {
            try {
                trailingSig_.update(input, offset, len);
            } catch (final SignatureException ex) {
                throw new AwsCryptoException(ex);
            }
        }
    }

    @Override
    public CiphertextHeaders getHeaders() {
        return ciphertextHeaders_;
    }

    @Override
    public List<K> getMasterKeys() {
        return Collections.singletonList(dataKey_.getMasterKey());
    }

    @Override
    public boolean isComplete() {
        return complete_;
    }
}
