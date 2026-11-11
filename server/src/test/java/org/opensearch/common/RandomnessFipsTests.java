/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.junit.After;

import java.security.Provider;
import java.security.Security;

public class RandomnessFipsTests extends RandomnessTests {

    private static final String BCFIPS = "BCFIPS";

    @After
    void restoreFipsProvider() throws Exception {
        if (Security.getProvider(BCFIPS) == null) {
            Class<?> clazz = Class.forName("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider");
            Provider bcFips = (Provider) clazz.getConstructor().newInstance();
            Security.insertProviderAt(bcFips, 1);
        }
    }

    public void testCreateSecure() {
        assertEquals("BCFIPS_RNG", Randomness.createSecure().getProvider().getName());
    }

    public void testCreateSecureFailsWithoutTheFipsProvider() {
        // Without BCFIPS no approved generator is left: the FIPS java.security names only BCFIPS in securerandom.strongAlgorithms,
        // so createSecure() fails instead of falling back to a non-approved SUN generator.
        Security.removeProvider(BCFIPS);

        final SecurityException e = expectThrows(SecurityException.class, Randomness::createSecure);

        assertTrue(e.getMessage().startsWith("Failed to instantiate SecureRandom"));
    }
}
