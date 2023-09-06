/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.crypto;

import java.io.InputStream;
import java.util.function.UnaryOperator;

/**
 * Contains adjusted range of partial encrypted content which needs to be used for decryption.
 */
public class DecryptedRangedStreamProvider {

    private final long[] adjustedRange;
    private final UnaryOperator<InputStream> decryptedStreamProvider;

    /**
     * To construct adjusted encrypted range.
     * @param adjustedRange range of partial encrypted content which needs to be used for decryption.
     * @param decryptedStreamProvider stream provider for decryption and range re-adjustment.
     */
    public DecryptedRangedStreamProvider(long[] adjustedRange, UnaryOperator<InputStream> decryptedStreamProvider) {
        this.adjustedRange = adjustedRange;
        this.decryptedStreamProvider = decryptedStreamProvider;
    }

    /**
     * Adjusted range of partial encrypted content which needs to be used for decryption.
     * @return adjusted range
     */
    public long[] getAdjustedRange() {
        return adjustedRange;
    }

    /**
     * A utility stream provider which supplies the stream responsible for decrypting the content and reading the
     * desired range of decrypted content by skipping extra content which got decrypted as a result of range adjustment.
     * @return stream provider for decryption and supplying the desired range of content.
     */
    public UnaryOperator<InputStream> getDecryptedStreamProvider() {
        return decryptedStreamProvider;
    }

}
