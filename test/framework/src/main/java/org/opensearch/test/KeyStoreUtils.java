/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.bouncycastle.asn1.LocaleUtil;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.opensearch.fips.FipsMode;

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.CertificateBuilder.Algorithm;
import io.netty.pkitesting.X509Bundle;

public class KeyStoreUtils {

    public static final char[] KEYSTORE_PASSWORD = "keystore_password".toCharArray();
    public static final Map<String, List<String>> TYPE_TO_EXTENSION_MAP = new HashMap<>();

    static {
        TYPE_TO_EXTENSION_MAP.put("JKS", List.of(".jks", ".ks"));
        TYPE_TO_EXTENSION_MAP.put("PKCS12", List.of(".p12", ".pkcs12", ".pfx"));
        TYPE_TO_EXTENSION_MAP.put("BCFKS", List.of(".bcfks")); // Bouncy Castle FIPS Keystore
    }

    /**
     * Make a best guess about the "type" (see {@link KeyStore#getType()}) of the keystore file located at the given {@code Path}.
     * This method only references the <em>file name</em> of the keystore, it does not look at its contents.
     */
    public static String inferStoreType(String filePath) {
        return TYPE_TO_EXTENSION_MAP.entrySet()
            .stream()
            .filter(entry -> entry.getValue().stream().anyMatch(filePath::endsWith))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown keystore type for file path: " + filePath));
    }

    public static KeyStore createServerKeyStore() throws Exception {
        var serverCred = generateCert();
        var keyStore = KeyStore.getInstance(FipsMode.CHECK.isFipsEnabled() ? "BCFKS" : "JKS");
        keyStore.load(null, null);
        keyStore.setKeyEntry(
            "server-ca",
            serverCred.getKeyPair().getPrivate(),
            KEYSTORE_PASSWORD,
            new X509Certificate[] { serverCred.getCertificate() }
        );
        return keyStore;
    }

    private static X509Bundle generateCert() throws Exception {
        final Locale locale = Locale.getDefault();
        try {
            Locale.setDefault(LocaleUtil.EN_Locale);
            // must use FIPS-approved algorithms like rsa2048, rsa3072, rsa4096, rsa8192, ecp256, ecp384
            // reference: https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/4943
            return new CertificateBuilder().subject("CN=Test CA Certificate")
                .setIsCertificateAuthority(true)
                .algorithm(Algorithm.rsa2048)
                .provider(new BouncyCastleFipsProvider())
                .buildSelfSigned();
        } finally {
            Locale.setDefault(locale);
        }
    }

}
