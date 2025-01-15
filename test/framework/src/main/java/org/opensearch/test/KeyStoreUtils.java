/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v1CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.opensearch.common.ssl.KeyStoreFactory;
import org.opensearch.common.ssl.KeyStoreType;

import javax.security.auth.x500.X500Principal;
import javax.security.auth.x500.X500PrivateCredential;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;

public class KeyStoreUtils {

    public static final char[] KEYSTORE_PASSWORD = "keystore_password".toCharArray();

    public static KeyStore createServerKeyStore() throws Exception {
        var serverCred = createCredential();
        var keyStore = KeyStoreFactory.getInstance(KeyStoreType.BCFKS);
        keyStore.load(null, null);
        keyStore.setKeyEntry(
            serverCred.getAlias(),
            serverCred.getPrivateKey(),
            KEYSTORE_PASSWORD,
            new X509Certificate[] { serverCred.getCertificate() }
        );
        return keyStore;
    }

    private static X500PrivateCredential createCredential() throws Exception {
        var keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        var keyPair = keyPairGenerator.generateKeyPair();
        var rootCert = new JcaX509CertificateConverter().getCertificate(generateCert(keyPair));
        return new X500PrivateCredential(rootCert, keyPair.getPrivate(), "server-ca");
    }

    private static X509CertificateHolder generateCert(KeyPair pair) throws Exception {
        var baseTime = System.currentTimeMillis();
        // 10 years in milliseconds
        var validityPeriod = 10L * 365 * 24 * 60 * 60 * 1000;

        var certBuilder = new JcaX509v1CertificateBuilder(
            new X500Principal("CN=Test CA Certificate"),
            BigInteger.valueOf(1),
            new Date(baseTime),
            new Date(baseTime + validityPeriod),
            new X500Principal("CN=Test CA Certificate"),
            pair.getPublic()
        );
        var signer = new JcaContentSignerBuilder("SHA256withRSA").build(pair.getPrivate());
        return certBuilder.build(signer);
    }

}
