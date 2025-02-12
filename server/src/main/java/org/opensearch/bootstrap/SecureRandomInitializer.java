/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsDRBG;
import org.bouncycastle.crypto.util.BasicEntropySourceProvider;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

/**
 * Instantiates {@link SecureRandom}
 */
public class SecureRandomInitializer {

    private SecureRandomInitializer() {}

    /**
     * Instantiates a new {@link SecureRandom} if it is not already set. The specific implementation used depends on whether the JVM
     * is running in a FIPS-approved mode or not. An instance of {@link SecureRandom} can be obtained from
     * {@link CryptoServicesRegistrar#getSecureRandom()}
     */
    public static void init() {
        CryptoServicesRegistrar.setSecureRandom(CryptoServicesRegistrar.getSecureRandomIfSet(SecureRandomInitializer::getSecureRandom));
    }

    private static SecureRandom getSecureRandom() {
        try {
            if (CryptoServicesRegistrar.isInApprovedOnlyMode()) {
                var entropySource = SecureRandom.getInstance("DEFAULT", "BCFIPS");
                return FipsDRBG.SHA512_HMAC.fromEntropySource(new BasicEntropySourceProvider(entropySource, true)).build(null, true);
            }
            return SecureRandom.getInstanceStrong();
        } catch (GeneralSecurityException e) {
            throw new SecurityException("Failed to instantiate SecureRandom: " + e.getMessage(), e);
        }
    }

}
