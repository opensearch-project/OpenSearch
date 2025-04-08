/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.security.NoSuchAlgorithmException;
import java.security.Security;

public class RandomnessTests extends OpenSearchTestCase {

    private static final String originalStrongAlgos = Security.getProperty("securerandom.strongAlgorithms");

    @After
    void restore() {
        if (originalStrongAlgos != null) {
            Security.setProperty("securerandom.strongAlgorithms", originalStrongAlgos);
        }

        if (Security.getProvider(BouncyCastleFipsProvider.PROVIDER_NAME) == null) {
            Security.insertProviderAt(new BouncyCastleFipsProvider(), 1);
        }
    }

    public void testCreateSecure() {
        assertEquals(inFipsJvm() ? "BCFIPS_RNG" : "SUN", Randomness.createSecure().getProvider().getName());
    }

    public void testFailsCreateSecureRandomWithoutStrongAlgos() {
        assumeFalse("Exception can only be thrown in non FIPS JVM", inFipsJvm());
        Security.setProperty("securerandom.strongAlgorithms", "NON_EXISTENT_ALGO");
        SecurityException ex = assertThrows(SecurityException.class, Randomness::createSecure);
        assertTrue(ex.getCause() instanceof NoSuchAlgorithmException);
    }

    public void testRecoveryAfterFailsCreateSecureInFipsJvm() {
        assumeTrue("Tests in FIPS mode with BC on classpath", inFipsJvm());
        // remove the BouncyCastle provider so that reflection throws a ReflectiveOperationException,
        // and the default SUN SecureRandom implementation is returned.
        Security.removeProvider(BouncyCastleFipsProvider.PROVIDER_NAME);
        assertEquals("SUN", Randomness.createSecure().getProvider().getName());
    }
}
