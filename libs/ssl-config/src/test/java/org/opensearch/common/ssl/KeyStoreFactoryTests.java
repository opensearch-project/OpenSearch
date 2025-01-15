/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.ssl;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class KeyStoreFactoryTests extends OpenSearchTestCase {

    public void testPKCS12KeyStore() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12).getType(), equalTo("PKCS12"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12).getProvider().getName(), equalTo("BCFIPS"));

        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12, "BCFIPS").getType(), equalTo("PKCS12"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12, "BCFIPS").getProvider().getName(), equalTo("BCFIPS"));

        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12, "SUN").getType(), equalTo("PKCS12"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.PKCS_12, "SUN").getProvider().getName(), equalTo("SUN"));

        KeyStoreType.TYPE_TO_EXTENSION_MAP.get(KeyStoreType.PKCS_12).forEach(extension -> {
            var keyStore = KeyStoreFactory.getInstanceBasedOnFileExtension(createRandomFileName(extension));
            assertThat(keyStore.getType(), equalTo(KeyStoreType.PKCS_12.getJcaName()));
        });
    }

    public void testJKSKeyStore() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.JKS).getType(), equalTo("JKS"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.JKS).getProvider().getName(), equalTo("SUN"));

        assertThat(KeyStoreFactory.getInstance(KeyStoreType.JKS, "SUN").getType(), equalTo("JKS"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.JKS, "SUN").getProvider().getName(), equalTo("SUN"));

        assertThrows("BCFKS not found", SecurityException.class, () -> KeyStoreFactory.getInstance(KeyStoreType.JKS, "BCFIPS"));

        KeyStoreType.TYPE_TO_EXTENSION_MAP.get(KeyStoreType.JKS).forEach(extension -> {
            var keyStore = KeyStoreFactory.getInstanceBasedOnFileExtension(createRandomFileName(extension));
            assertThat(keyStore.getType(), equalTo(KeyStoreType.JKS.getJcaName()));
        });
    }

    public void testBCFIPSKeyStore() {
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.BCFKS).getType(), equalTo("BCFKS"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.BCFKS).getProvider().getName(), equalTo("BCFIPS"));

        assertThat(KeyStoreFactory.getInstance(KeyStoreType.BCFKS, "BCFIPS").getType(), equalTo("BCFKS"));
        assertThat(KeyStoreFactory.getInstance(KeyStoreType.BCFKS, "BCFIPS").getProvider().getName(), equalTo("BCFIPS"));

        KeyStoreType.TYPE_TO_EXTENSION_MAP.get(KeyStoreType.BCFKS).forEach(extension -> {
            var keyStore = KeyStoreFactory.getInstanceBasedOnFileExtension(createRandomFileName(extension));
            assertThat(keyStore.getType(), equalTo(KeyStoreType.BCFKS.getJcaName()));
        });
    }

    public void testUnknownKeyStoreType() {
        assertThrows(
            "Unknown keystore type for file path: keystore.unknown",
            IllegalArgumentException.class,
            () -> KeyStoreFactory.getInstanceBasedOnFileExtension(createRandomFileName("unknown"))
        );
    }

    private String createRandomFileName(String extension) {
        return RandomStrings.randomAsciiAlphanumOfLengthBetween(random(), 0, 10) + "." + extension;
    }
}
