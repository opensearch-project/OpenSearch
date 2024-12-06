/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.bouncycastle.crypto.CryptoServicesRegistrar;
import org.junit.After;
import org.junit.Before;

import javax.crypto.Cipher;

import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;

public class SecurityProviderManagerTests extends LuceneTestCase {

    private static final String BC_FIPS = "BCFIPS";
    private static final String SUN_JCE = "SunJCE";

    // BCFIPS will only provide legacy ciphers when running in general mode, otherwise approved-only mode forbids the use.
    private static final String MODE_DEPENDENT_CIPHER_PROVIDER = CryptoServicesRegistrar.isInApprovedOnlyMode() ? SUN_JCE : BC_FIPS;
    private static final Provider[] originalProviders = Security.getProviders();

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
        assumeTrue(
            "SunJCE provider needs to be available",
            Arrays.stream(Security.getProviders()).anyMatch(provider -> SUN_JCE.equals(provider.getName()))
        );
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        for (Provider provider : Security.getProviders()) {
            Security.removeProvider(provider.getName());
        }

        for (Provider provider : originalProviders) {
            Security.addProvider(provider);
        }
    }

    public void testCipherRC4() throws Exception {
        // given
        var cipher = Cipher.getInstance(RC_4);
        assertEquals(RC_4, cipher.getAlgorithm());
        assertEquals(MODE_DEPENDENT_CIPHER_PROVIDER, cipher.getProvider().getName());

        // when
        SecurityProviderManager.excludeSunJCE();

        // then
        if (CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(RC_4));
        } else {
            cipher = Cipher.getInstance(RC_4);
            assertEquals(RC_4, cipher.getAlgorithm());
            assertEquals(BC_FIPS, cipher.getProvider().getName());
        }
    }

    public void testCipherAES() throws Exception {
        // given
        var cipher = Cipher.getInstance(AES);
        assertEquals(AES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());

        // when
        SecurityProviderManager.excludeSunJCE();

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
        SecurityProviderManager.excludeSunJCE();

        // then
        cipher = Cipher.getInstance(TRIPLE_DES);
        assertEquals(TRIPLE_DES, cipher.getAlgorithm());
        assertEquals(BC_FIPS, cipher.getProvider().getName());
    }

    public void testCipherDes() throws Exception {
        // given
        var cipher = Cipher.getInstance(DES);
        assertEquals(DES, cipher.getAlgorithm());
        assertEquals(MODE_DEPENDENT_CIPHER_PROVIDER, cipher.getProvider().getName());

        // when
        SecurityProviderManager.excludeSunJCE();

        // then
        if (CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(DES));
        } else {
            cipher = Cipher.getInstance(DES);
            assertEquals(DES, cipher.getAlgorithm());
            assertEquals(BC_FIPS, cipher.getProvider().getName());
        }
    }

    public void testCipherPBE() throws Exception {
        // given
        var cipher = Cipher.getInstance(PBE);
        assertEquals(PBE, cipher.getAlgorithm());
        assertEquals(SUN_JCE, cipher.getProvider().getName());

        // when
        SecurityProviderManager.excludeSunJCE();

        // then
        expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(PBE));
    }

    public void testCipherBlowfish() throws Exception {
        // given
        var cipher = Cipher.getInstance(BLOWFISH);
        assertEquals(BLOWFISH, cipher.getAlgorithm());
        assertEquals(MODE_DEPENDENT_CIPHER_PROVIDER, cipher.getProvider().getName());

        // when
        SecurityProviderManager.excludeSunJCE();

        // then
        if (CryptoServicesRegistrar.isInApprovedOnlyMode()) {
            expectThrows(NoSuchAlgorithmException.class, () -> Cipher.getInstance(BLOWFISH));
        } else {
            cipher = Cipher.getInstance(BLOWFISH);
            assertEquals(BLOWFISH, cipher.getAlgorithm());
            assertEquals(BC_FIPS, cipher.getProvider().getName());
        }
    }

}
