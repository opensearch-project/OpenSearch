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

package org.opensearch.repositories.gcs;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.SecurityUtils;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.crypto.KeyStoreFactory;
import org.opensearch.common.crypto.KeyStoreType;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;

import java.io.IOException;
import java.io.InputStream;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.net.URI;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class GoogleCloudStorageService {

    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageService.class);

    private volatile Map<String, GoogleCloudStorageClientSettings> clientSettings = emptyMap();

    /**
     * Dictionary of client instances. Client instances are built lazily from the
     * latest settings. Each repository has its own client instance identified by
     * the repository name.
     */
    private volatile Map<String, Storage> clientCache = emptyMap();

    final private GoogleApplicationDefaultCredentials googleApplicationDefaultCredentials;

    public GoogleCloudStorageService() {
        this.googleApplicationDefaultCredentials = new GoogleApplicationDefaultCredentials();
    }

    public GoogleCloudStorageService(GoogleApplicationDefaultCredentials googleApplicationDefaultCredentials) {
        this.googleApplicationDefaultCredentials = googleApplicationDefaultCredentials;
    }

    /**
     * Refreshes the client settings and clears the client cache. Subsequent calls to
     * {@code GoogleCloudStorageService#client} will return new clients constructed
     * using the parameter settings.
     *
     * @param clientsSettings the new settings used for building clients for subsequent requests
     */
    public synchronized void refreshAndClearCache(Map<String, GoogleCloudStorageClientSettings> clientsSettings) {
        this.clientCache = emptyMap();
        this.clientSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
    }

    /**
     * Attempts to retrieve a client from the cache. If the client does not exist it
     * will be created from the latest settings and will populate the cache. The
     * returned instance should not be cached by the calling code. Instead, for each
     * use, the (possibly updated) instance should be requested by calling this
     * method.
     *
     * @param clientName name of the client settings used to create the client
     * @param repositoryName name of the repository that would use the client
     * @param stats the stats collector used to gather information about the underlying SKD API calls.
     * @return a cached client storage instance that can be used to manage objects
     *         (blobs)
     */
    public Storage client(final String clientName, final String repositoryName, final GoogleCloudStorageOperationsStats stats)
        throws IOException {
        {
            final Storage storage = clientCache.get(repositoryName);
            if (storage != null) {
                return storage;
            }
        }
        synchronized (this) {
            final Storage existing = clientCache.get(repositoryName);

            if (existing != null) {
                return existing;
            }

            final GoogleCloudStorageClientSettings settings = clientSettings.get(clientName);

            if (settings == null) {
                throw new IllegalArgumentException(
                    "Unknown client name ["
                        + clientName
                        + "]. Existing client configs: "
                        + Strings.collectionToDelimitedString(clientSettings.keySet(), ",")
                );
            }

            logger.debug(
                () -> new ParameterizedMessage("creating GCS client with client_name [{}], endpoint [{}]", clientName, settings.getHost())
            );
            final Storage storage = createClient(settings, stats);
            clientCache = MapBuilder.newMapBuilder(clientCache).put(repositoryName, storage).immutableMap();
            return storage;
        }
    }

    synchronized void closeRepositoryClient(String repositoryName) {
        clientCache = MapBuilder.newMapBuilder(clientCache).remove(repositoryName).immutableMap();
    }

    /**
     * Creates a client that can be used to manage Google Cloud Storage objects. The client is thread-safe.
     *
     * @param clientSettings client settings to use, including secure settings
     * @param stats the stats collector to use by the underlying SDK
     * @return a new client storage instance that can be used to manage objects
     *         (blobs)
     */
    private Storage createClient(GoogleCloudStorageClientSettings clientSettings, GoogleCloudStorageOperationsStats stats)
        throws IOException {
        final HttpTransport httpTransport = createHttpTransport(clientSettings);

        final GoogleCloudStorageHttpStatsCollector httpStatsCollector = new GoogleCloudStorageHttpStatsCollector(stats);

        final HttpTransportOptions httpTransportOptions = new HttpTransportOptions(
            HttpTransportOptions.newBuilder()
                .setConnectTimeout(toTimeout(clientSettings.getConnectTimeout()))
                .setReadTimeout(toTimeout(clientSettings.getReadTimeout()))
                .setHttpTransportFactory(() -> httpTransport)
        ) {

            @Override
            public HttpRequestInitializer getHttpRequestInitializer(ServiceOptions<?, ?> serviceOptions) {
                HttpRequestInitializer requestInitializer = super.getHttpRequestInitializer(serviceOptions);

                return (httpRequest) -> {
                    if (requestInitializer != null) requestInitializer.initialize(httpRequest);

                    httpRequest.setResponseInterceptor(httpStatsCollector);
                };
            }
        };

        final StorageOptions storageOptions = createStorageOptions(clientSettings, httpTransportOptions);
        return storageOptions.getService();
    }

    private HttpTransport createHttpTransport(final GoogleCloudStorageClientSettings clientSettings) throws IOException {
        return SocketAccess.doPrivilegedIOException(() -> {
            final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
            // use the BCFIPS trustStore format instead of PKCS#12 to ensure compatibility with BC-FIPS
            var certTrustStore = KeyStoreFactory.getInstance(KeyStoreType.BCFKS);
            InputStream keyStoreStream = getClass().getResourceAsStream("/google.bcfks");
            SecurityUtils.loadKeyStore(certTrustStore, keyStoreStream, "notasecret");

            builder.trustCertificates(certTrustStore);
            final ProxySettings proxySettings = clientSettings.getProxySettings();
            if (proxySettings != ProxySettings.NO_PROXY_SETTINGS) {
                if (proxySettings.isAuthenticated()) {
                    Authenticator.setDefault(new Authenticator() {
                        @Override
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(proxySettings.getUsername(), proxySettings.getPassword().toCharArray());
                        }
                    });
                }
                builder.setProxy(new Proxy(proxySettings.getType(), proxySettings.getAddress()));
            }
            return builder.build();
        });
    }

    StorageOptions createStorageOptions(
        final GoogleCloudStorageClientSettings clientSettings,
        final HttpTransportOptions httpTransportOptions
    ) {
        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
            .setTransportOptions(httpTransportOptions)
            .setHeaderProvider(() -> {
                final MapBuilder<String, String> mapBuilder = MapBuilder.newMapBuilder();
                if (Strings.hasLength(clientSettings.getApplicationName())) {
                    mapBuilder.put("user-agent", clientSettings.getApplicationName());
                }
                return mapBuilder.immutableMap();
            });
        if (Strings.hasLength(clientSettings.getHost())) {
            storageOptionsBuilder.setHost(clientSettings.getHost());
        }
        if (Strings.hasLength(clientSettings.getProjectId())) {
            storageOptionsBuilder.setProjectId(clientSettings.getProjectId());
        }
        if (clientSettings.getCredential() == null) {
            logger.info("\"Application Default Credentials\" will be in use");
            final GoogleCredentials credentials = googleApplicationDefaultCredentials.get();
            if (credentials != null) {
                storageOptionsBuilder.setCredentials(credentials);
            }
        } else {
            ServiceAccountCredentials serviceAccountCredentials = clientSettings.getCredential();
            // override token server URI
            final URI tokenServerUri = clientSettings.getTokenUri();
            if (Strings.hasLength(tokenServerUri.toString())) {
                // Rebuild the service account credentials in order to use a custom Token url.
                // This is mostly used for testing purpose.
                serviceAccountCredentials = serviceAccountCredentials.toBuilder().setTokenServerUri(tokenServerUri).build();
            }
            storageOptionsBuilder.setCredentials(serviceAccountCredentials);
        }
        return SocketAccess.doPrivilegedException(() -> storageOptionsBuilder.build());
    }

    /**
     * Converts timeout values from the settings to a timeout value for the Google
     * Cloud SDK
     **/
    static Integer toTimeout(final TimeValue timeout) {
        // Null or zero in settings means the default timeout
        if (timeout == null || TimeValue.ZERO.equals(timeout)) {
            // negative value means using the default value
            return -1;
        }
        // -1 means infinite timeout
        if (TimeValue.MINUS_ONE.equals(timeout)) {
            // 0 is the infinite timeout expected by Google Cloud SDK
            return 0;
        }
        return Math.toIntExact(timeout.getMillis());
    }
}
