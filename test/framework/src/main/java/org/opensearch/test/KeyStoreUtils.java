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

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Locale;

import io.netty.pkitesting.CertificateBuilder;
import io.netty.pkitesting.CertificateBuilder.Algorithm;
import io.netty.pkitesting.X509Bundle;

public class KeyStoreUtils {

    public static final char[] KEYSTORE_PASSWORD = "keystore_password".toCharArray();

    public static KeyStore createServerKeyStore() throws Exception {
        var serverCred = generateCert();
        var keyStore = KeyStore.getInstance("JKS");
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
            return new CertificateBuilder().subject("CN=Test CA Certificate")
                .setIsCertificateAuthority(true)
                .algorithm(Algorithm.ed25519)
                .provider(new BouncyCastleFipsProvider())
                .buildSelfSigned();
        } finally {
            Locale.setDefault(locale);
        }
    }

}
