/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.opensearch.fips.FipsMode;

import javax.security.auth.x500.X500Principal;
import javax.security.auth.x500.X500PrivateCredential;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
        var serverCred = createCredential();
        var keyStore = KeyStore.getInstance(FipsMode.CHECK.isFipsEnabled() ? "BCFKS" : "JKS");
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

        var certBuilder = new X509v1CertificateBuilder(
            X500Name.getInstance(new X500Principal("CN=Test CA Certificate").getEncoded()),
            BigInteger.valueOf(1),
            new Date(baseTime),
            new Date(baseTime + validityPeriod),
            Locale.ROOT,
            X500Name.getInstance(new X500Principal("CN=Test CA Certificate").getEncoded()),
            SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded())
        );
        var signer = new JcaContentSignerBuilder("SHA256withRSA").build(pair.getPrivate());
        return certBuilder.build(signer);
    }

}
