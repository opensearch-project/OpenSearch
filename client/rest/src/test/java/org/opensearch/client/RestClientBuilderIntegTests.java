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

package org.opensearch.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.PrivilegedAction;
import java.security.SecureRandom;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Integration test to validate the builder builds a client with the correct configuration
 */
public class RestClientBuilderIntegTests extends RestClientTestCase {

    private static HttpsServer httpsServer;

    @BeforeClass
    public static void startHttpServer() throws Exception {
        httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpsServer.setHttpsConfigurator(new HttpsConfigurator(getSslContext(true)));
        httpsServer.createContext("/", new ResponseHandler());
        httpsServer.start();
    }

    private static class ResponseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            httpExchange.sendResponseHeaders(200, -1);
            httpExchange.close();
        }
    }

    @AfterClass
    public static void stopHttpServers() throws IOException {
        httpsServer.stop(0);
        httpsServer = null;
    }

    public void testBuilderUsesDefaultSSLContext() throws Exception {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/49094", inFipsJvm());
        final SSLContext defaultSSLContext = SSLContext.getDefault();
        try {
            try (RestClient client = buildRestClient()) {
                try {
                    client.performRequest(new Request("GET", "/"));
                    fail("connection should have been rejected due to SSL handshake");
                } catch (Exception e) {
                    assertThat(e.getCause(), instanceOf(SSLException.class));
                }
            }

            SSLContext.setDefault(getSslContext(false));
            try (RestClient client = buildRestClient()) {
                Response response = client.performRequest(new Request("GET", "/"));
                assertEquals(200, response.getStatusLine().getStatusCode());
            }
        } finally {
            SSLContext.setDefault(defaultSSLContext);
        }
    }

    private RestClient buildRestClient() {
        InetSocketAddress address = httpsServer.getAddress();
        return RestClient.builder(new HttpHost("https", address.getHostString(), address.getPort())).build();
    }

    private static SSLContext getSslContext(boolean server) throws Exception {
        SSLContext sslContext;
        char[] password = "password".toCharArray();
        SecureRandom secureRandom = SecureRandom.getInstance("DEFAULT", "BCFIPS");
        String fileExtension = ".jks";

        try (
            InputStream trustStoreFile = RestClientBuilderIntegTests.class.getResourceAsStream("/test_truststore" + fileExtension);
            InputStream keyStoreFile = RestClientBuilderIntegTests.class.getResourceAsStream("/testks" + fileExtension)
        ) {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(keyStoreFile, password);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX", "BCJSSE");
            kmf.init(keyStore, password);

            KeyStore trustStore = KeyStore.getInstance("JKS");
            trustStore.load(trustStoreFile, password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX", "BCJSSE");
            tmf.init(trustStore);

            SSLContextBuilder sslContextBuilder = SSLContextBuilder.create()
                .setProvider("BCJSSE")
                .setProtocol(getProtocol())
                .setSecureRandom(secureRandom);

            if (server) {
                sslContextBuilder.loadKeyMaterial(keyStore, password);
            }
            sslContextBuilder.loadTrustMaterial(trustStore, null);
            sslContext = sslContextBuilder.build();

        }
        return sslContext;
    }

    /**
     * The {@link HttpsServer} in the JDK has issues with TLSv1.3 when running in a JDK that supports TLSv1.3 prior to
     * 12.0.1 so we pin to TLSv1.2 when running on an earlier JDK.
     */
    private static String getProtocol() {
        String version = AccessController.doPrivileged((PrivilegedAction<String>) () -> System.getProperty("java.version"));
        String[] parts = version.split("-");
        String[] numericComponents;
        if (parts.length == 1) {
            numericComponents = version.split("\\.");
        } else if (parts.length == 2) {
            numericComponents = parts[0].split("\\.");
        } else {
            throw new IllegalArgumentException("Java version string [" + version + "] could not be parsed.");
        }
        if (numericComponents.length > 0) {
            final int major = Integer.valueOf(numericComponents[0]);
            if (major < 11) {
                return "TLS";
            }
            if (major > 12) {
                return "TLS";
            } else if (major == 12 && numericComponents.length > 2) {
                final int minor = Integer.valueOf(numericComponents[1]);
                if (minor > 0) {
                    return "TLS";
                } else {
                    String patch = numericComponents[2];
                    final int index = patch.indexOf("_");
                    if (index > -1) {
                        patch = patch.substring(0, index);
                    }

                    if (Integer.valueOf(patch) >= 1) {
                        return "TLS";
                    }
                }
            }
        }
        return "TLSv1.2";
    }
}
