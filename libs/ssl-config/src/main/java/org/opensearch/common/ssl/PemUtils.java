/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.ssl;

import org.bouncycastle.asn1.ASN1ObjectIdentifier;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEInputDecryptorProviderBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

final class PemUtils {

    public static final String BCFIPS = "BCFIPS";

    PemUtils() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * Creates a {@link PrivateKey} from the contents of a file. Supports PKCS#1, PKCS#8
     * encoded formats of encrypted and plaintext RSA, DSA and EC(secp256r1) keys.
     *
     * @param keyPath           the path for the key file
     * @param passwordSupplier A password supplier for the potentially encrypted (password protected) key. Unencrypted keys ignore this value.
     * @return a private key from the contents of the file
     */
    public static PrivateKey readPrivateKey(Path keyPath, Supplier<char[]> passwordSupplier) throws IOException, PKCSException {
        PrivateKeyInfo pki = loadPrivateKeyFromFile(keyPath, passwordSupplier);
        JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
        return converter.getPrivateKey(pki);
    }

    static List<Certificate> readCertificates(Collection<Path> certPaths) throws CertificateException, IOException {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        List<Certificate> certificates = new ArrayList<>(certPaths.size());
        for (Path path : certPaths) {
            try (InputStream input = Files.newInputStream(path)) {
                final Collection<? extends Certificate> parsed = certFactory.generateCertificates(input);
                if (parsed.isEmpty()) {
                    throw new SslConfigException("Failed to parse any certificate from [" + path.toAbsolutePath() + "]");
                }
                certificates.addAll(parsed);
            }
        }
        return certificates;
    }

    /**
     * Creates a {@link PrivateKey} from the private key, with or without encryption.
     * When enforcing the approved-only mode in Java security settings, some functionalities might be restricted due to the limited
     * set of allowed algorithms. One such restriction includes Password Based Key Derivation Functions (PBKDF) like those used by OpenSSL
     * and PKCS#12 formats. Since these formats rely on PBKDF algorithms, they cannot operate correctly within the approved-only mode.
     * Consequently, attempting to utilize them could result in a {@link java.security.NoSuchAlgorithmException}.
     *
     * @param passwordSupplier The password supplier for the encrypted (password protected) key
     * @return {@link PrivateKey}
     * @throws IOException If the file can't be read
     */
    private static PrivateKeyInfo loadPrivateKeyFromFile(Path keyPath, Supplier<char[]> passwordSupplier) throws IOException,
        PKCSException {

        try (PEMParser pemParser = new PEMParser(Files.newBufferedReader(keyPath, StandardCharsets.UTF_8))) {
            Object object = readObject(keyPath, pemParser);

            if (object instanceof PKCS8EncryptedPrivateKeyInfo) { // encrypted private key in pkcs8-format
                var privateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) object;
                var inputDecryptorProvider = new JcePKCSPBEInputDecryptorProviderBuilder().setProvider(BCFIPS)
                    .build(passwordSupplier.get());
                return privateKeyInfo.decryptPrivateKeyInfo(inputDecryptorProvider);
            } else if (object instanceof PEMEncryptedKeyPair) { // encrypted private key
                var encryptedKeyPair = (PEMEncryptedKeyPair) object;
                var decryptorProvider = new JcePEMDecryptorProviderBuilder().setProvider(BCFIPS).build(passwordSupplier.get());
                var keyPair = encryptedKeyPair.decryptKeyPair(decryptorProvider);
                return keyPair.getPrivateKeyInfo();
            } else if (object instanceof PEMKeyPair) { // unencrypted private key
                return ((PEMKeyPair) object).getPrivateKeyInfo();
            } else if (object instanceof PrivateKeyInfo) { // unencrypted private key in pkcs8-format
                return (PrivateKeyInfo) object;
            } else {
                throw new SslConfigException(
                    String.format(
                        Locale.ROOT,
                        "error parsing private key [%s], invalid encrypted private key class: [%s]",
                        keyPath.toAbsolutePath(),
                        object.getClass().getName()
                    )
                );
            }
        }
    }

    /**
     * Supports PEM files that includes parameters.
     *
     * @return high-level Object from the content
     */
    private static Object readObject(Path keyPath, PEMParser pemParser) throws IOException {
        while (pemParser.ready()) {
            try {
                var object = pemParser.readObject();
                if (object == null) { // ignore unknown objects;
                    continue;
                }
                if (object instanceof ASN1ObjectIdentifier) { // ignore -----BEGIN EC PARAMETERS-----
                    continue;
                }
                return object;
            } catch (IOException e) { // ignore -----BEGIN DSA PARAMETERS-----
                // ignore
            }
        }
        throw new SslConfigException(
            "Error parsing Private Key [" + keyPath.toAbsolutePath() + "]. The file is empty, or does not contain expected key format."
        );
    }

}
