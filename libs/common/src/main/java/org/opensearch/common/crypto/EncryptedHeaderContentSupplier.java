/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.common.crypto;

import java.io.IOException;

/**
 * This is used in partial decryption. Header information is required for decryption of actual encrypted content.
 * Implementation of this supplier only requires first few bytes of encrypted content to be supplied.
 */
public interface EncryptedHeaderContentSupplier {

    /**
     * @param start Start position of the encrypted content (Generally supplied as 0 during usage)
     * @param end End position of the header.
     * @return Encrypted header content (May contain additional content which is later discarded)
     * @throws IOException In case content fetch fails.
     */
    byte[] supply(long start, long end) throws IOException;
}
