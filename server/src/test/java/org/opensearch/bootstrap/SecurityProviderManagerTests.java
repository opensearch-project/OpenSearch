/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import javax.crypto.Cipher;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;

public class SecurityProviderManagerTests extends OpenSearchTestCase {

    private static final String BC_FIPS = "BCFIPS";
    private static final String SUN_JCE = "SunJCE";
    private static final String TOP_PRIO_CIPHER_PROVIDER = inFipsJvm() ? BC_FIPS : SUN_JCE;
    private static final String AES = "AES";
    private static final String RC_4 = "RC4";
    private static final String TRIPLE_DES = "DESedeWrap";
    private static final String DES = "DES";
    private static final String PBE = "PBE";
    private static final String BLOWFISH = "Blowfish";

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        addSunJceProvider();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        if (!inFipsJvm()) {
            addSunJceProvider();
        }
        super.tearDown();
    }

    @AfterClass
    // restore the same state as before running the tests.
    public static void afterClass() throws Exception {
        if (inFipsJvm()) {
            SecurityProviderManager.removeNonCompliantFipsProviders();
        }
    }

    public void testCipherRC4() throws Exception {
        // given
        var cipher = Cipher.getInstance(RC_4);
        assertEquals(RC_4, cipher.getAlgorithm());
        assertEquals(SUN_JCE, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(RC_4));
    }

    public void testCipherAES() throws Exception {
        // given
        var cipher = Cipher.getInstance(AES);
        assertEquals(AES, cipher.getAlgorithm());
        assertEquals(TOP_PRIO_CIPHER_PROVIDER, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        if (inFipsJvm()) {
            cipher = Cipher.getInstance(AES);
            assertEquals(AES, cipher.getAlgorithm());
            assertEquals(BC_FIPS, cipher.getProvider().getName());
        } else {
            expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(AES));
        }
    }

    public void testCipher3Des() throws Exception {
        // given
        var cipher = Cipher.getInstance(TRIPLE_DES);
        assertEquals(TRIPLE_DES, cipher.getAlgorithm());
        assertEquals(TOP_PRIO_CIPHER_PROVIDER, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        if (inFipsJvm()) {
            cipher = Cipher.getInstance(TRIPLE_DES);
            assertEquals(TRIPLE_DES, cipher.getAlgorithm());
            assertEquals(BC_FIPS, cipher.getProvider().getName());
        } else {
            expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(TRIPLE_DES));
        }
    }

    public void testCipherDes() throws Exception {
        // given
        var cipher = Cipher.getInstance(DES);
        assertEquals(DES, cipher.getAlgorithm());
        assertEquals(SUN_JCE, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(DES));
    }

    public void testCipherPBE() throws Exception {
        // given
        var cipher = Cipher.getInstance(PBE);
        assertEquals(PBE, cipher.getAlgorithm());
        assertEquals(SUN_JCE, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(PBE));
    }

    public void testCipherBlowfish() throws Exception {
        // given
        var cipher = Cipher.getInstance(BLOWFISH);
        assertEquals(BLOWFISH, cipher.getAlgorithm());
        assertEquals(SUN_JCE, cipher.getProvider().getName());

        // when
        SecurityProviderManager.removeNonCompliantFipsProviders();

        // then
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(BLOWFISH));
    }

    public void testGetPosition() {
        assertTrue(SUN_JCE + " is installed", SecurityProviderManager.getPosition(SUN_JCE) > 0);
        SecurityProviderManager.removeNonCompliantFipsProviders();
        assertTrue(SUN_JCE + " is uninstalled", SecurityProviderManager.getPosition(SUN_JCE) < 0);
    }

    private static void addSunJceProvider() throws Exception {
        if (Arrays.stream(Security.getProviders()).noneMatch(provider -> SUN_JCE.equals(provider.getName()))) {
            var sunJceClass = Class.forName("com.sun.crypto.provider.SunJCE");
            var originalSunProvider = (Provider) sunJceClass.getConstructor().newInstance();
            Security.addProvider(originalSunProvider);
        }
    }

}
