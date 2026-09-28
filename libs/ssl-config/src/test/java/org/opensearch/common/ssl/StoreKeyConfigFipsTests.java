/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import org.hamcrest.Matchers;

import javax.net.ssl.KeyManagerFactory;

import java.nio.file.Path;

public class StoreKeyConfigFipsTests extends StoreKeyConfigTests {

    protected static final char[] BCFKS_PASS = "bcfks-pass".toCharArray();
    protected static final String BCFKS = "BCFKS";

    public void testLoadSingleKeyPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadMultipleKeyPKCS12() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadMultipleKeyJksWithSeparateKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testKeyManagerFailsWithIncorrectJksStorePassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testKeyManagerFailsWithIncorrectJksKeyPassword() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testMissingKeyEntriesFailsForJksWithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testMissingKeyEntriesFailsForP12WithMeaningfulMessage() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testKeyConfigReloadsFileContentsForP12Keystore() throws Exception {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
    }

    public void testLoadMultipleKeyBcfks() throws Exception {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(bcfks, BCFKS_PASS, BCFKS, BCFKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertKeysLoaded(keyConfig, "cert1", "cert2");
    }

    public void testKeyManagerFailsWithIncorrectBcfksStorePassword() throws Exception {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(bcfks, P12_PASS, BCFKS, BCFKS_PASS, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertPasswordIsIncorrect(keyConfig, bcfks);
    }

    public void testKeyManagerFailsWithIncorrectBcfksKeyPassword() throws Exception {
        final Path bcfks = getDataPath("/certs/cert-all/certs.bcfks");
        final StoreKeyConfig keyConfig = new StoreKeyConfig(
            bcfks,
            BCFKS_PASS,
            BCFKS,
            "nonsense".toCharArray(),
            KeyManagerFactory.getDefaultAlgorithm()
        );
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(bcfks));
        assertPasswordIsIncorrect(keyConfig, bcfks);
    }

    public void testMissingKeyEntriesFailsForBcfksWithMeaningfulMessage() throws Exception {
        final Path ks = getDataPath("/certs/ca-all/ca.bcfks");
        final char[] password = BCFKS_PASS;
        final StoreKeyConfig keyConfig = new StoreKeyConfig(ks, password, BCFKS, password, KeyManagerFactory.getDefaultAlgorithm());
        assertThat(keyConfig.getDependentFiles(), Matchers.containsInAnyOrder(ks));
        assertNoPrivateKeyEntries(keyConfig, ks);
    }
}
