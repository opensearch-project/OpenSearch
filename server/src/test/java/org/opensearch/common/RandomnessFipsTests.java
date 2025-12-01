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

    public void testRecoveryAfterFailsCreateSecureInFipsJvm() {
        // remove the BouncyCastle provider so that reflection throws a ReflectiveOperationException,
        // and the default SUN SecureRandom implementation is returned.
        Security.removeProvider(BCFIPS);
        assertEquals(SUN, Randomness.createSecure().getProvider().getName());
    }
}
