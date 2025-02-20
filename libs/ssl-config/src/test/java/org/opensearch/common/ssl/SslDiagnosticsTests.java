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

import org.opensearch.common.Nullable;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;

import java.io.IOException;
import java.nio.file.Path;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.mockito.Mockito;

public class SslDiagnosticsTests extends OpenSearchTestCase {

    // Some constants for use in mock certificates
    private static final byte[] MOCK_ENCODING_1 = { 0x61, 0x62, 0x63, 0x64, 0x65, 0x66 };
    private static final String MOCK_FINGERPRINT_1 = "1f8ac10f23c5b5bc1167bda84b833e5c057a77d2";
    private static final byte[] MOCK_ENCODING_2 = { 0x62, 0x63, 0x64, 0x65, 0x66, 0x67 };
    private static final String MOCK_FINGERPRINT_2 = "836d472783f4a210cfa3ab5621f757d1a2964aca";
    private static final byte[] MOCK_ENCODING_3 = { 0x63, 0x64, 0x65, 0x66, 0x67, 0x68 };
    private static final String MOCK_FINGERPRINT_3 = "da8e062d74919f549a9764c24ab0fcde3af3719f";
    private static final byte[] MOCK_ENCODING_4 = { 0x64, 0x65, 0x66, 0x67, 0x68, 0x69 };
    private static final String MOCK_FINGERPRINT_4 = "5d96965bfae50bf2be0d6259eb87a6cc9f5d0b26";

    public void testTrustEmptyStore() {
        var fileName = "cert-all/empty.jks";
        var exception = assertThrows(SslConfigException.class, () -> loadCertificate(fileName));
        assertThat(
            exception.getMessage(),
            Matchers.equalTo(
                String.format(Locale.ROOT, "Failed to parse any certificate from [%s]", getDataPath("/certs/" + fileName).toAbsolutePath())
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesAFullCertChainThatIsTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt", "ca1/ca.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1/ca.crt", "ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1];"
                    + " the certificate is signed by"
                    + " (subject [CN=Test CA 1] fingerprint [2b7b0416391bdf86502505c23149022d2213dadc] {trusted issuer})"
                    + " which is self-issued; the [CN=Test CA 1] certificate is trusted in this ssl context ([foo.http.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesAFullCertChainThatIsntTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt", "ca1/ca.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1];"
                    + " the certificate is signed by (subject [CN=Test CA 1] fingerprint [2b7b0416391bdf86502505c23149022d2213dadc])"
                    + " which is self-issued; the [CN=Test CA 1] certificate is not trusted in this ssl context ([foo.http.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerFullCertChainIsntTrustedButMimicIssuerExists() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt", "ca1/ca.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1-b/ca.crt", "ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1];"
                    + " the certificate is signed by (subject [CN=Test CA 1] fingerprint [2b7b0416391bdf86502505c23149022d2213dadc])"
                    + " which is self-issued; the [CN=Test CA 1] certificate is not trusted in this ssl context ([foo.http.ssl]);"
                    + " this ssl context does trust a certificate with subject [CN=Test CA 1]"
                    + " but the trusted certificate has fingerprint [b095bf2526be20783e1f26dfd69c7aae910e3663]"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesEndCertificateOnlyAndTheCertAuthIsTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1/ca.crt", "ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1]"
                    + " but the server did not provide a copy of the issuing certificate in the certificate chain;"
                    + " the issuing certificate with fingerprint [2b7b0416391bdf86502505c23149022d2213dadc]"
                    + " is trusted in this ssl context ([foo.http.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesEndCertificateOnlyButTheCertAuthIsNotTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1]"
                    + " but the server did not provide a copy of the issuing certificate in the certificate chain;"
                    + " this ssl context ([foo.http.ssl]) is not configured to trust that issuer"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesEndCertificateOnlyWithMimicIssuer() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1-b/ca.crt", "ca2/ca.crt", "ca3/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1]"
                    + " but the server did not provide a copy of the issuing certificate in the certificate chain;"
                    + " this ssl context ([foo.http.ssl]) trusts [1] certificate with subject name [CN=Test CA 1]"
                    + " and fingerprint [b095bf2526be20783e1f26dfd69c7aae910e3663] but the signatures do not match"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesEndCertificateWithMultipleMimicIssuers() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt");
        final SSLSession session = session("192.168.1.9");
        final X509Certificate ca1b = loadCertificate("ca1-b/ca.crt");
        final Map<String, List<X509Certificate>> trustIssuers = trust(ca1b, cloneCertificateAsMock(ca1b));
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.9];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1]"
                    + " but the server did not provide a copy of the issuing certificate in the certificate chain;"
                    + " this ssl context ([foo.http.ssl]) trusts [2] certificates with subject name [CN=Test CA 1]"
                    + " and fingerprint [b095bf2526be20783e1f26dfd69c7aae910e3663], fingerprint ["
                    + MOCK_FINGERPRINT_1
                    + "]"
                    + " but the signatures do not match"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidePartialChainFromTrustedCA() throws Exception {
        final X509Certificate rootCA = mockCertificateWithIssuer(
            "CN=root-ca,DC=example,DC=com",
            MOCK_ENCODING_1,
            Collections.emptyList(),
            null
        );
        final X509Certificate issuingCA = mockCertificateWithIssuer(
            "CN=issuing-ca,DC=example,DC=com",
            MOCK_ENCODING_2,
            Collections.emptyList(),
            rootCA
        );
        final X509Certificate localCA = mockCertificateWithIssuer(
            "CN=ca,OU=windows,DC=example,DC=com",
            MOCK_ENCODING_3,
            Collections.emptyList(),
            issuingCA
        );
        final X509Certificate endCert = mockCertificateWithIssuer(
            "CN=elastic1,OU=windows,DC=example,DC=com",
            MOCK_ENCODING_4,
            Collections.emptyList(),
            localCA
        );

        final X509Certificate[] chain = { endCert, localCA, issuingCA };

        final SSLSession session = session("192.168.1.5");
        final Map<String, List<X509Certificate>> trustIssuers = trust(issuingCA, rootCA);
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.security.authc.realms.ldap.ldap1.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.5];"
                    + " the server provided a certificate with subject name [CN=elastic1,OU=windows,DC=example,DC=com]"
                    + " and fingerprint ["
                    + MOCK_FINGERPRINT_4
                    + "];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is issued by [CN=ca,OU=windows,DC=example,DC=com];"
                    + " the certificate is"
                    + " signed by (subject [CN=ca,OU=windows,DC=example,DC=com] fingerprint ["
                    + MOCK_FINGERPRINT_3
                    + "])"
                    + " signed by (subject [CN=issuing-ca,DC=example,DC=com] fingerprint ["
                    + MOCK_FINGERPRINT_2
                    + "] {trusted issuer})"
                    + " which is issued by [CN=root-ca,DC=example,DC=com] (but that issuer certificate was not provided in the chain);"
                    + " the issuing certificate with fingerprint ["
                    + MOCK_FINGERPRINT_1
                    + "]"
                    + " is trusted in this ssl context ([foo.security.authc.realms.ldap.ldap1.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidePartialChainFromUntrustedCA() throws Exception {
        final X509Certificate rootCA = mockCertificateWithIssuer(
            "CN=root-ca,DC=example,DC=com",
            MOCK_ENCODING_1,
            Collections.emptyList(),
            null
        );
        final X509Certificate issuingCA = mockCertificateWithIssuer(
            "CN=issuing-ca,DC=example,DC=com",
            MOCK_ENCODING_2,
            Collections.emptyList(),
            rootCA
        );
        final X509Certificate localCA = mockCertificateWithIssuer(
            "CN=ca,OU=windows,DC=example,DC=com",
            MOCK_ENCODING_3,
            Collections.emptyList(),
            issuingCA
        );
        final X509Certificate endCert = mockCertificateWithIssuer(
            "CN=elastic1,OU=windows,DC=example,DC=com",
            MOCK_ENCODING_4,
            Collections.emptyList(),
            localCA
        );

        final X509Certificate[] chain = { endCert, localCA, issuingCA };

        final SSLSession session = session("192.168.1.6");
        final Map<String, List<X509Certificate>> trustIssuers = trust(Collections.emptyList());
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.security.authc.realms.ldap.ldap1.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.6];"
                    + " the server provided a certificate with subject name [CN=elastic1,OU=windows,DC=example,DC=com]"
                    + " and fingerprint ["
                    + MOCK_FINGERPRINT_4
                    + "];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is issued by [CN=ca,OU=windows,DC=example,DC=com];"
                    + " the certificate is"
                    + " signed by (subject [CN=ca,OU=windows,DC=example,DC=com] fingerprint ["
                    + MOCK_FINGERPRINT_3
                    + "])"
                    + " signed by (subject [CN=issuing-ca,DC=example,DC=com] fingerprint ["
                    + MOCK_FINGERPRINT_2
                    + "])"
                    + " which is issued by [CN=root-ca,DC=example,DC=com] (but that issuer certificate was not provided in the chain);"
                    + " this ssl context ([foo.security.authc.realms.ldap.ldap1.ssl]) is not configured to trust that issuer"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesASelfSignedCertThatIsDirectlyTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("ca1/ca.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1/ca.crt", "ca2/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=Test CA 1]"
                    + " and fingerprint [2b7b0416391bdf86502505c23149022d2213dadc];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is self-issued; the [CN=Test CA 1] certificate is trusted in this ssl context ([foo.http.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesASelfSignedCertThatIsNotTrusted() throws Exception {
        X509Certificate[] chain = loadCertChain("ca1/ca.crt");
        final SSLSession session = session("192.168.10.10");
        final Map<String, List<X509Certificate>> trustIssuers = Collections.emptyMap();
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.10.10];"
                    + " the server provided a certificate with subject name [CN=Test CA 1]"
                    + " and fingerprint [2b7b0416391bdf86502505c23149022d2213dadc];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is self-issued; the [CN=Test CA 1] certificate is not trusted in this ssl context ([foo.http.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenServerProvidesASelfSignedCertWithMimicName() throws Exception {
        X509Certificate[] chain = loadCertChain("ca1/ca.crt");
        final SSLSession session = session("192.168.1.1");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1-b/ca.crt", "ca2/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.1];"
                    + " the server provided a certificate with subject name [CN=Test CA 1]"
                    + " and fingerprint [2b7b0416391bdf86502505c23149022d2213dadc];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is self-issued; the [CN=Test CA 1] certificate is not trusted in this ssl context ([foo.http.ssl]);"
                    + " this ssl context does trust a certificate with subject [CN=Test CA 1]"
                    + " but the trusted certificate has fingerprint [b095bf2526be20783e1f26dfd69c7aae910e3663]"
            )
        );
    }

    public void testDiagnosticMessageWithEmptyChain() throws Exception {
        X509Certificate[] chain = new X509Certificate[0];
        final SSLSession session = session("192.168.1.2");
        final Map<String, List<X509Certificate>> trustIssuers = Collections.emptyMap();
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.http.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo("failed to establish trust with server at [192.168.1.2];" + " the server did not provide a certificate")
        );
    }

    public void testDiagnosticMessageWhenServerProvidesAnEmailSubjAltName() throws Exception {
        final String subjectName = "CN=foo,DC=example,DC=com";
        final X509Certificate certificate = mockCertificateWithIssuer(
            subjectName,
            MOCK_ENCODING_1,
            Collections.singletonList(Arrays.asList(1, "foo@example.com")),
            null
        );
        X509Certificate[] chain = new X509Certificate[] { certificate };

        final SSLSession session = session("192.168.1.3");
        final Map<String, List<X509Certificate>> trustIssuers = trust(certificate);
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.monitoring.exporters.opensearch.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.3];"
                    + " the server provided a certificate with subject name [CN=foo,DC=example,DC=com]"
                    + " and fingerprint ["
                    + MOCK_FINGERPRINT_1
                    + "];"
                    + " the certificate does not have any DNS/IP subject alternative names;"
                    + " the certificate is self-issued;"
                    + " the [CN=foo,DC=example,DC=com] certificate is trusted in this ssl context ([foo.monitoring.exporters.opensearch.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenACertificateHasAnInvalidEncoding() throws Exception {
        final String subjectName = "CN=foo,DC=example,DC=com";
        final X509Certificate certificate = mockCertificateWithIssuer(subjectName, new byte[0], Collections.emptyList(), null);
        Mockito.when(certificate.getEncoded()).thenThrow(new CertificateEncodingException("MOCK INVALID ENCODING"));
        X509Certificate[] chain = new X509Certificate[] { certificate };

        final SSLSession session = session("192.168.1.6");
        final Map<String, List<X509Certificate>> trustIssuers = trust(Collections.emptyList());
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.security.transport.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.6];"
                    + " the server provided a certificate with subject name [CN=foo,DC=example,DC=com]"
                    + " and invalid encoding [java.security.cert.CertificateEncodingException: MOCK INVALID ENCODING];"
                    + " the certificate does not have any subject alternative names;"
                    + " the certificate is self-issued;"
                    + " the [CN=foo,DC=example,DC=com] certificate is not trusted in this ssl context ([foo.security.transport.ssl])"
            )
        );
    }

    public void testDiagnosticMessageForClientCertificate() throws Exception {
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt");

        final SSLSession session = session("192.168.1.7");
        final Map<String, List<X509Certificate>> trustIssuers = trust("ca1/ca.crt");
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.CLIENT,
            session,
            "foo.security.transport.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with client at [192.168.1.7];"
                    + " the client provided a certificate with subject name [CN=cert1]"
                    + " and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate is issued by [CN=Test CA 1]"
                    + " but the client did not provide a copy of the issuing certificate in the certificate chain;"
                    + " the issuing certificate with fingerprint [2b7b0416391bdf86502505c23149022d2213dadc]"
                    + " is trusted in this ssl context ([foo.security.transport.ssl])"
            )
        );
    }

    public void testDiagnosticMessageWhenCaHasNewIssuingCertificate() throws Exception {
        // From time to time, CAs issue updated certificates based on the same underlying key-pair.
        // For example, they might move to new signature algorithms (dropping SHA-1), or the certificate might be
        // expiring and need to be reissued with a new expiry date.
        // In this test, we assume that the server provides a certificate that is signed by the new CA cert, and we trust the old CA cert
        // Our diagnostic message should make clear that we trust the CA, but using a different cert fingerprint.
        // Note: This would normally succeed, so we wouldn't have an exception to diagnose, but it's possible that the hostname is wrong.
        final X509Certificate newCaCert = loadCertificate("ca1/ca.crt");
        final X509Certificate oldCaCert = cloneCertificateAsMock(newCaCert);
        X509Certificate[] chain = loadCertChain("cert1/cert1.crt", "ca1/ca.crt"); // uses "new" CA

        final SSLSession session = session("192.168.1.4");
        final Map<String, List<X509Certificate>> trustIssuers = trust(oldCaCert);
        final String message = SslDiagnostics.getTrustDiagnosticFailure(
            chain,
            SslDiagnostics.PeerType.SERVER,
            session,
            "foo.security.authc.realms.saml.saml1.ssl",
            trustIssuers
        );
        assertThat(
            message,
            Matchers.equalTo(
                "failed to establish trust with server at [192.168.1.4];"
                    + " the server provided a certificate with subject name [CN=cert1] and fingerprint [7e0919348e566651a136f2a1d5974585d5b3712e];"
                    + " the certificate has subject alternative names [DNS:localhost,IP:127.0.0.1];"
                    + " the certificate is issued by [CN=Test CA 1];"
                    + " the certificate is signed by (subject [CN=Test CA 1]"
                    + " fingerprint [2b7b0416391bdf86502505c23149022d2213dadc] {trusted issuer})"
                    + " which is self-issued;"
                    + " the [CN=Test CA 1] certificate is trusted in this ssl context ([foo.security.authc.realms.saml.saml1.ssl])"
                    + " because we trust a certificate with fingerprint [1f8ac10f23c5b5bc1167bda84b833e5c057a77d2]"
                    + " for the same public key"
            )
        );
    }

    public X509Certificate cloneCertificateAsMock(X509Certificate clone) throws CertificateParsingException, CertificateEncodingException {
        final X509Certificate cert = Mockito.mock(X509Certificate.class);
        final X500Principal principal = clone.getSubjectX500Principal();
        Mockito.when(cert.getSubjectX500Principal()).thenReturn(principal);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(clone.getSubjectAlternativeNames());
        Mockito.when(cert.getIssuerX500Principal()).thenReturn(clone.getIssuerX500Principal());
        Mockito.when(cert.getPublicKey()).thenReturn(clone.getPublicKey());
        Mockito.when(cert.getEncoded()).thenReturn(new byte[] { 0x61, 0x62, 0x63, 0x64, 0x65, 0x66 });
        return cert;
    }

    public X509Certificate mockCertificateWithIssuer(
        String principal,
        byte[] encoding,
        List<List<?>> subjAltNames,
        @Nullable X509Certificate issuer
    ) throws CertificateException {

        final X509Certificate cert = Mockito.mock(X509Certificate.class);
        final X500Principal x500Principal = new X500Principal(principal);
        final PublicKey key = Mockito.mock(PublicKey.class);

        Mockito.when(cert.getSubjectX500Principal()).thenReturn(x500Principal);
        Mockito.when(cert.getSubjectAlternativeNames()).thenReturn(subjAltNames);
        final X500Principal issuerPrincipal = issuer == null ? x500Principal : issuer.getSubjectX500Principal();
        Mockito.when(cert.getIssuerX500Principal()).thenReturn(issuerPrincipal);
        Mockito.when(cert.getPublicKey()).thenReturn(key);
        Mockito.when(cert.getEncoded()).thenReturn(encoding);

        return cert;
    }

    private X509Certificate[] loadCertChain(String... names) throws CertificateException, IOException {
        final List<Path> paths = Stream.of(names).map(p -> "/certs/" + p).map(this::getDataPath).collect(Collectors.toList());
        return PemUtils.readCertificates(paths).stream().map(X509Certificate.class::cast).toArray(X509Certificate[]::new);
    }

    private X509Certificate loadCertificate(String name) throws CertificateException, IOException {
        final Path path = getDataPath("/certs/" + name);
        final List<Certificate> certificates = PemUtils.readCertificates(Collections.singleton(path));
        if (certificates.size() == 1) {
            return (X509Certificate) certificates.get(0);
        } else {
            throw new IllegalStateException(
                "Expected 1 certificate in [" + path.toAbsolutePath() + "] but found [" + certificates.size() + "] - " + certificates
            );
        }
    }

    private Map<String, List<X509Certificate>> trust(String... certNames) throws CertificateException, IOException {
        final List<Path> paths = Stream.of(certNames).map(p -> "/certs/" + p).map(this::getDataPath).collect(Collectors.toList());
        return trust(PemUtils.readCertificates(paths));
    }

    private Map<String, List<X509Certificate>> trust(X509Certificate... caCerts) {
        return trust(Arrays.asList(caCerts));
    }

    private Map<String, List<X509Certificate>> trust(Collection<? extends Certificate> caCerts) {
        return caCerts.stream()
            .map(X509Certificate.class::cast)
            .collect(
                Collectors.toMap(
                    x -> x.getSubjectX500Principal().getName(),
                    Collections::singletonList,
                    (List<X509Certificate> a, List<X509Certificate> b) -> {
                        List<X509Certificate> merge = new ArrayList<>();
                        merge.addAll(a);
                        merge.addAll(b);
                        return merge;
                    }
                )
            );
    }

    private SSLSession session(String peerHost) {
        final SSLSession mock = Mockito.mock(SSLSession.class);
        Mockito.when(mock.getPeerHost()).thenReturn(peerHost);
        return mock;
    }
}
