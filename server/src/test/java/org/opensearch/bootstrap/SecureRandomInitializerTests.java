/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.bouncycastle.crypto.fips.FipsSecureRandom;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.security.SecureRandom;
import java.util.Arrays;

public class SecureRandomInitializerTests extends OpenSearchTestCase {

    @BeforeClass
    public static void setup() {
        CryptoServicesRegistrar.setSecureRandom(null);
    }

    public void testInitInNonFipsMode() {
        // given
        assertThrows(IllegalStateException.class, CryptoServicesRegistrar::getSecureRandom);

        // when
        SecureRandomInitializer.init();
        SecureRandom secureRandom = CryptoServicesRegistrar.getSecureRandom();

        // then
        assertNotNull("SecureRandom should be initialized in non-FIPS mode", secureRandom);
        byte[] randomBytes = new byte[16];
        secureRandom.nextBytes(randomBytes);
        assertEquals(inFipsJvm() ? "BCFIPS_RNG" : "SUN", secureRandom.getProvider().getName());
        // BCFIPS 'DEFAULT' RNG algorithm defaults to 'HMAC-DRBG-SHA512'
        assertEquals(inFipsJvm() ? "HMAC-DRBG-SHA512" : "NativePRNGBlocking", secureRandom.getAlgorithm());
        assertEquals(inFipsJvm() ? FipsSecureRandom.class : SecureRandom.class, secureRandom.getClass());
        assertFalse("Random bytes should not be all zeros", allZeros(randomBytes));

        byte[] seed1 = secureRandom.generateSeed(16);
        byte[] seed2 = secureRandom.generateSeed(16);
        assertNotNull(seed1);
        assertNotNull(seed2);
        assertFalse("Seeds should not be identical", Arrays.equals(seed1, seed2));
    }

    private boolean allZeros(byte[] data) {
        for (byte b : data) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }
}
