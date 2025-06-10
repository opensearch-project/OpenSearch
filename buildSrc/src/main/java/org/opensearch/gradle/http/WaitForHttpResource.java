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

package org.opensearch.gradle.http;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A utility to wait for a specific HTTP resource to be available, optionally with customized TLS trusted CAs.
 * This is logically similar to using the Ant Get task to retrieve a resource, but with the difference that it can
 * access resources that do not use the JRE's default trusted CAs.
 */
public class WaitForHttpResource {

    private static final Logger logger = Logging.getLogger(WaitForHttpResource.class);

    private Set<Integer> validResponseCodes = Collections.singleton(200);
    private URL url;
    private Set<File> certificateAuthorities;
    private File trustStoreFile;
    private String trustStorePassword;
    private String username;
    private String password;

    public WaitForHttpResource(String protocol, String host, int numberOfNodes) throws MalformedURLException {
        this(new URL(protocol + "://" + host + "/_cluster/health?wait_for_nodes=>=" + numberOfNodes + "&wait_for_status=yellow"));
    }

    public WaitForHttpResource(String protocol, String host, String username, String password, int numberOfNodes)
        throws MalformedURLException {
        this(
            new URL(
                protocol
                    + "://"
                    + username
                    + ":"
                    + password
                    + "@"
                    + host
                    + "/_cluster/health?wait_for_nodes=>="
                    + numberOfNodes
                    + "&wait_for_status=yellow"
            )
        );
    }

    public WaitForHttpResource(URL url) {
        this.url = url;
    }

    public void setValidResponseCodes(int... validResponseCodes) {
        this.validResponseCodes = new HashSet<>(validResponseCodes.length);
        for (int rc : validResponseCodes) {
            this.validResponseCodes.add(rc);
        }
    }

    public void setCertificateAuthorities(File... certificateAuthorities) {
        this.certificateAuthorities = new HashSet<>(Arrays.asList(certificateAuthorities));
    }

    public void setTrustStoreFile(File trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean wait(int durationInMs) throws GeneralSecurityException, InterruptedException, IOException {
        final long waitUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(durationInMs);
        final long sleep = Long.max(durationInMs / 10, 100);

        final SSLContext ssl;
        final KeyStore trustStore = buildTrustStore();
        if (trustStore != null) {
            ssl = createSslContext(trustStore);
        } else {
            ssl = null;
        }
        IOException failure = null;
        while (true) {
            try {
                checkResource(ssl);
                return true;
            } catch (IOException e) {
                logger.debug("Failed to access resource [{}]", url, e);
                failure = e;
            }
            if (System.nanoTime() < waitUntil) {
                Thread.sleep(sleep);
            } else {
                throw failure;
            }
        }
    }

    protected void checkResource(SSLContext ssl) throws IOException {
        final HttpURLConnection connection = buildConnection(ssl);
        connection.connect();
        final Integer response = connection.getResponseCode();
        if (validResponseCodes.contains(response)) {
            logger.info("Got successful response [{}] from URL [{}]", response, url);
            return;
        } else {
            throw new IOException(response + " " + connection.getResponseMessage());
        }
    }

    HttpURLConnection buildConnection(SSLContext ssl) throws IOException {
        final HttpURLConnection connection = (HttpURLConnection) this.url.openConnection();
        configureSslContext(connection, ssl);
        configureBasicAuth(connection);
        connection.setRequestMethod("GET");
        return connection;
    }

    private void configureSslContext(HttpURLConnection connection, SSLContext ssl) {
        if (ssl != null) {
            if (connection instanceof HttpsURLConnection) {
                ((HttpsURLConnection) connection).setSSLSocketFactory(ssl.getSocketFactory());
            } else {
                throw new IllegalStateException("SSL trust has been configured, but [" + url + "] is not a 'https' URL");
            }
        }
    }

    private void configureBasicAuth(HttpURLConnection connection) {
        if (username != null) {
            if (password == null) {
                throw new IllegalStateException("Basic Auth user [" + username + "] has been set, but no password has been configured");
            }
            connection.setRequestProperty(
                "Authorization",
                "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8))
            );
        }
    }

    KeyStore buildTrustStore() throws GeneralSecurityException, IOException {
        if (this.certificateAuthorities != null) {
            if (trustStoreFile != null) {
                throw new IllegalStateException("Cannot specify both truststore and CAs");
            }
            return buildTrustStoreFromCA();
        } else if (trustStoreFile != null) {
            return buildTrustStoreFromFile();
        } else {
            return null;
        }
    }

    private KeyStore buildTrustStoreFromFile() throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance(trustStoreFile.getName().endsWith(".jks") ? "JKS" : "PKCS12");
        try (InputStream input = new FileInputStream(trustStoreFile)) {
            keyStore.load(input, trustStorePassword == null ? null : trustStorePassword.toCharArray());
        }
        return keyStore;
    }

    private KeyStore buildTrustStoreFromCA() throws GeneralSecurityException, IOException {
        final KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        final CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        int counter = 0;
        for (File ca : certificateAuthorities) {
            try (InputStream input = new FileInputStream(ca)) {
                for (Certificate certificate : certFactory.generateCertificates(input)) {
                    store.setCertificateEntry("cert-" + counter, certificate);
                    counter++;
                }
            }
        }
        return store;
    }

    private SSLContext createSslContext(KeyStore trustStore) throws GeneralSecurityException {
        checkForTrustEntry(trustStore);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(new KeyManager[0], tmf.getTrustManagers(), new SecureRandom());
        return sslContext;
    }

    private void checkForTrustEntry(KeyStore trustStore) throws KeyStoreException {
        Enumeration<String> enumeration = trustStore.aliases();
        while (enumeration.hasMoreElements()) {
            if (trustStore.isCertificateEntry(enumeration.nextElement())) {
                // found trusted cert entry
                return;
            }
        }
        throw new IllegalStateException("Trust-store does not contain any trusted certificate entries");
    }
}
