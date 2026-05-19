/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.junit.After;
import org.junit.AfterClass;

import javax.crypto.Cipher;

import java.security.Provider;
import java.security.Security;
import java.util.Arrays;

public class SecurityProviderManagerFipsTests extends SecurityProviderManagerTests {

    @After
    @Override
    public void tearDown() throws Exception {
        addSunJceProvider();
        super.tearDown();
    }

    @AfterClass
    // restore the same state as before running the tests.
    public static void afterClass() throws Exception {
        SecurityProviderManager.removeNonCompliantFipsProviders();
    }

    public void testCipherAES() throws Exception {
        // given
        var cipher = Cipher.getInstance(AES);
        assertEquals(AES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        cipher = Cipher.getInstance(AES);
        assertEquals(AES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());
    }

    public void testCipher3Des() throws Exception {
        // given
        var cipher = Cipher.getInstance(TRIPLE_DES);
        assertEquals(TRIPLE_DES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        cipher = Cipher.getInstance(TRIPLE_DES);
        assertEquals(TRIPLE_DES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());
    }

    protected static void addSunJceProvider() throws Exception {
        if (Arrays.stream(java.security.Security.getProviders()).noneMatch(provider -> SUN_JCE.equals(provider.getName()))) {
            var sunJceClass = Class.forName("com.sun.crypto.provider.SunJCE");
            var originalSunProvider = (Provider) sunJceClass.getConstructor().newInstance();
            Security.addProvider(originalSunProvider);
        }
    }
}
