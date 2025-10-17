/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.security.Provider;
import java.security.Security;

public class RandomnessTests extends OpenSearchTestCase {

    private static final String BCFIPS = "BCFIPS";
    private static final String SUN = "SUN";
    private static final String originalStrongAlgos = Security.getProperty("securerandom.strongAlgorithms");

    @After
    void restore() throws Exception {
        if (originalStrongAlgos != null) {
            Security.setProperty("securerandom.strongAlgorithms", originalStrongAlgos);
        }

        if (inFipsJvm() && Security.getProvider(BCFIPS) == null) {
            Class<?> clazz = Class.forName("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider");
            Provider bcFips = (Provider) clazz.getConstructor().newInstance();
            Security.insertProviderAt(bcFips, 1);
        }
    }

    public void testCreateSecure() {
        assertEquals(inFipsJvm() ? "BCFIPS_RNG" : SUN, Randomness.createSecure().getProvider().getName());
    }

    public void testRecoveryAfterFailsCreateSecureInFipsJvm() {
        assumeTrue("Tests in FIPS mode with BC on classpath", inFipsJvm());
        // remove the BouncyCastle provider so that reflection throws a ReflectiveOperationException,
        // and the default SUN SecureRandom implementation is returned.
        Security.removeProvider(BCFIPS);
        assertEquals(SUN, Randomness.createSecure().getProvider().getName());
    }
}
