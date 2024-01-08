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

import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.CREDENTIALS_FILE_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.ENDPOINT_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.PROJECT_ID_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.getClientSettings;
import static org.opensearch.repositories.gcs.GoogleCloudStorageClientSettings.loadCredential;

public class GoogleCloudStorageClientSettingsTests extends OpenSearchTestCase {

    public void testLoadWithEmptySettings() {
        final Map<String, GoogleCloudStorageClientSettings> clientsSettings = GoogleCloudStorageClientSettings.load(Settings.EMPTY);
        assertEquals(1, clientsSettings.size());
        assertNotNull(clientsSettings.get("default"));
    }

    public void testLoad() throws Exception {
        final int nbClients = randomIntBetween(1, 5);
        final List<Setting<?>> deprecationWarnings = new ArrayList<>();
        final Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClients = randomClients(nbClients, deprecationWarnings);
        final Map<String, GoogleCloudStorageClientSettings> expectedClientsSettings = randomClients.v1();

        final Map<String, GoogleCloudStorageClientSettings> actualClientsSettings = GoogleCloudStorageClientSettings.load(
            randomClients.v2()
        );
        assertEquals(expectedClientsSettings.size(), actualClientsSettings.size());

        for (final String clientName : expectedClientsSettings.keySet()) {
            final GoogleCloudStorageClientSettings actualClientSettings = actualClientsSettings.get(clientName);
            assertNotNull(actualClientSettings);
            final GoogleCloudStorageClientSettings expectedClientSettings = expectedClientsSettings.get(clientName);
            assertNotNull(expectedClientSettings);
            assertGoogleCredential(expectedClientSettings.getCredential(), actualClientSettings.getCredential());
            assertEquals(expectedClientSettings.getHost(), actualClientSettings.getHost());
            assertEquals(expectedClientSettings.getProjectId(), actualClientSettings.getProjectId());
            assertEquals(expectedClientSettings.getConnectTimeout(), actualClientSettings.getConnectTimeout());
            assertEquals(expectedClientSettings.getReadTimeout(), actualClientSettings.getReadTimeout());
            assertEquals(expectedClientSettings.getApplicationName(), actualClientSettings.getApplicationName());
            assertEquals(ProxySettings.NO_PROXY_SETTINGS, actualClientSettings.getProxySettings());
        }

        if (deprecationWarnings.isEmpty() == false) {
            assertSettingDeprecationsAndWarnings(deprecationWarnings.toArray(new Setting<?>[0]));
        }
    }

    public void testLoadCredential() throws Exception {
        final List<Setting<?>> deprecationWarnings = new ArrayList<>();
        final Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClient = randomClients(1, deprecationWarnings);
        final GoogleCloudStorageClientSettings expectedClientSettings = randomClient.v1().values().iterator().next();
        final String clientName = randomClient.v1().keySet().iterator().next();
        assertGoogleCredential(expectedClientSettings.getCredential(), loadCredential(randomClient.v2(), clientName));
    }

    public void testProjectIdDefaultsToCredentials() throws Exception {
        final String clientName = randomAlphaOfLength(5);
        final Tuple<ServiceAccountCredentials, byte[]> credentials = randomCredential(clientName);
        final ServiceAccountCredentials credential = credentials.v1();
        final GoogleCloudStorageClientSettings googleCloudStorageClientSettings = new GoogleCloudStorageClientSettings(
            credential,
            ENDPOINT_SETTING.getDefault(Settings.EMPTY),
            PROJECT_ID_SETTING.getDefault(Settings.EMPTY),
            CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY),
            new URI(""),
            new ProxySettings(Proxy.Type.DIRECT, null, 0, null, null)
        );
        assertEquals(credential.getProjectId(), googleCloudStorageClientSettings.getProjectId());
    }

    public void testHttpProxySettings() throws Exception {
        final int port = randomIntBetween(10, 1080);
        final String userName = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        final GoogleCloudStorageClientSettings gcsWithHttpProxyWithoutUserPwd = proxyGoogleCloudStorageClientSettings(
            new ProxySettings(Proxy.Type.HTTP, InetAddress.getByName("127.0.0.10"), port, null, null)
        );

        assertEquals(Proxy.Type.HTTP, gcsWithHttpProxyWithoutUserPwd.getProxySettings().getType());
        assertEquals(
            new InetSocketAddress(InetAddress.getByName("127.0.0.10"), port),
            gcsWithHttpProxyWithoutUserPwd.getProxySettings().getAddress()
        );
        assertNull(gcsWithHttpProxyWithoutUserPwd.getProxySettings().getUsername());
        assertNull(gcsWithHttpProxyWithoutUserPwd.getProxySettings().getPassword());
        assertFalse(gcsWithHttpProxyWithoutUserPwd.getProxySettings().isAuthenticated());

        final GoogleCloudStorageClientSettings gcsWithHttpProxyWithUserPwd = proxyGoogleCloudStorageClientSettings(
            new ProxySettings(Proxy.Type.HTTP, InetAddress.getByName("127.0.0.10"), port, userName, password)
        );

        assertEquals(Proxy.Type.HTTP, gcsWithHttpProxyWithoutUserPwd.getProxySettings().getType());
        assertEquals(
            new InetSocketAddress(InetAddress.getByName("127.0.0.10"), port),
            gcsWithHttpProxyWithUserPwd.getProxySettings().getAddress()
        );
        assertTrue(gcsWithHttpProxyWithUserPwd.getProxySettings().isAuthenticated());
        assertEquals(userName, gcsWithHttpProxyWithUserPwd.getProxySettings().getUsername());
        assertEquals(password, gcsWithHttpProxyWithUserPwd.getProxySettings().getPassword());
    }

    public void testSocksProxySettings() throws Exception {
        final int port = randomIntBetween(10, 1080);
        final String userName = randomAlphaOfLength(10);
        final String password = randomAlphaOfLength(10);
        final GoogleCloudStorageClientSettings gcsWithHttpProxyWithoutUserPwd = proxyGoogleCloudStorageClientSettings(
            new ProxySettings(Proxy.Type.SOCKS, InetAddress.getByName("127.0.0.10"), port, null, null)
        );

        assertEquals(Proxy.Type.SOCKS, gcsWithHttpProxyWithoutUserPwd.getProxySettings().getType());
        assertEquals(
            new InetSocketAddress(InetAddress.getByName("127.0.0.10"), port),
            gcsWithHttpProxyWithoutUserPwd.getProxySettings().getAddress()
        );
        assertFalse(gcsWithHttpProxyWithoutUserPwd.getProxySettings().isAuthenticated());
        assertNull(gcsWithHttpProxyWithoutUserPwd.getProxySettings().getUsername());
        assertNull(gcsWithHttpProxyWithoutUserPwd.getProxySettings().getPassword());

        final GoogleCloudStorageClientSettings gcsWithHttpProxyWithUserPwd = proxyGoogleCloudStorageClientSettings(
            new ProxySettings(Proxy.Type.SOCKS, InetAddress.getByName("127.0.0.10"), port, userName, password)
        );

        assertEquals(Proxy.Type.SOCKS, gcsWithHttpProxyWithoutUserPwd.getProxySettings().getType());
        assertEquals(
            new InetSocketAddress(InetAddress.getByName("127.0.0.10"), port),
            gcsWithHttpProxyWithUserPwd.getProxySettings().getAddress()
        );
        assertTrue(gcsWithHttpProxyWithUserPwd.getProxySettings().isAuthenticated());
        assertEquals(userName, gcsWithHttpProxyWithUserPwd.getProxySettings().getUsername());
        assertEquals(password, gcsWithHttpProxyWithUserPwd.getProxySettings().getPassword());
    }

    public void testProxyWrongHost() {
        final Settings settings = Settings.builder()
            .put("gcs.client.default.proxy.type", randomFrom("socks", "http"))
            .put("gcs.client.default.proxy.host", "thisisnotavalidhostorwehavebeensuperunlucky")
            .put("gcs.client.default.proxy.port", 8080)
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> GoogleCloudStorageClientSettings.load(settings));
        assertEquals("Google Cloud Storage proxy host is unknown.", e.getMessage());
    }

    public void testProxyTypeNotSet() {
        final Settings hostPortSettings = Settings.builder()
            .put("gcs.client.default.proxy.host", "127.0.0.1")
            .put("gcs.client.default.proxy.port", 8080)
            .build();

        SettingsException e = expectThrows(SettingsException.class, () -> GoogleCloudStorageClientSettings.load(hostPortSettings));
        assertEquals(
            "Google Cloud Storage proxy port or host or username or password have been set but proxy type is not defined.",
            e.getMessage()
        );

        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("gcs.client.default.proxy.username", "aaaa");
        secureSettings.setString("gcs.client.default.proxy.password", "bbbb");
        final Settings usernamePasswordSettings = Settings.builder().setSecureSettings(secureSettings).build();

        e = expectThrows(SettingsException.class, () -> GoogleCloudStorageClientSettings.load(usernamePasswordSettings));
        assertEquals(
            "Google Cloud Storage proxy port or host or username or password have been set but proxy type is not defined.",
            e.getMessage()
        );
    }

    public void testProxyHostNotSet() {
        final Settings settings = Settings.builder()
            .put("gcs.client.default.proxy.port", 8080)
            .put("gcs.client.default.proxy.type", randomFrom("socks", "http"))
            .build();
        final SettingsException e = expectThrows(SettingsException.class, () -> GoogleCloudStorageClientSettings.load(settings));
        assertEquals("Google Cloud Storage proxy type has been set but proxy host or port is not defined.", e.getMessage());
    }

    private GoogleCloudStorageClientSettings proxyGoogleCloudStorageClientSettings(final ProxySettings proxySettings) throws Exception {
        final String clientName = randomAlphaOfLength(5);
        return new GoogleCloudStorageClientSettings(
            randomCredential(clientName).v1(),
            ENDPOINT_SETTING.getDefault(Settings.EMPTY),
            PROJECT_ID_SETTING.getDefault(Settings.EMPTY),
            CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY),
            new URI(""),
            proxySettings
        );
    }

    /** Generates a given number of GoogleCloudStorageClientSettings along with the Settings to build them from **/
    private Tuple<Map<String, GoogleCloudStorageClientSettings>, Settings> randomClients(
        final int nbClients,
        final List<Setting<?>> deprecationWarnings
    ) throws Exception {
        final Map<String, GoogleCloudStorageClientSettings> expectedClients = new HashMap<>();

        final Settings.Builder settings = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();

        for (int i = 0; i < nbClients; i++) {
            final String clientName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            final GoogleCloudStorageClientSettings clientSettings = randomClient(clientName, settings, secureSettings, deprecationWarnings);
            expectedClients.put(clientName, clientSettings);
        }

        if (randomBoolean()) {
            final GoogleCloudStorageClientSettings clientSettings = randomClient("default", settings, secureSettings, deprecationWarnings);
            expectedClients.put("default", clientSettings);
        } else {
            expectedClients.put("default", getClientSettings(Settings.EMPTY, "default"));
        }

        return Tuple.tuple(expectedClients, settings.setSecureSettings(secureSettings).build());
    }

    /** Generates a random GoogleCloudStorageClientSettings along with the Settings to build it **/
    private static GoogleCloudStorageClientSettings randomClient(
        final String clientName,
        final Settings.Builder settings,
        final MockSecureSettings secureSettings,
        final List<Setting<?>> deprecationWarnings
    ) throws Exception {

        final Tuple<ServiceAccountCredentials, byte[]> credentials = randomCredential(clientName);
        final ServiceAccountCredentials credential = credentials.v1();
        secureSettings.setFile(CREDENTIALS_FILE_SETTING.getConcreteSettingForNamespace(clientName).getKey(), credentials.v2());

        String endpoint;
        if (randomBoolean()) {
            endpoint = randomFrom(
                "http://www.opensearch.org",
                "http://metadata.google.com:88/oauth",
                "https://www.googleapis.com",
                "https://www.opensearch.org:443",
                "http://localhost:8443",
                "https://www.googleapis.com/oauth/token"
            );
            settings.put(ENDPOINT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), endpoint);
        } else {
            endpoint = ENDPOINT_SETTING.getDefault(Settings.EMPTY);
        }

        String projectId;
        if (randomBoolean()) {
            projectId = randomAlphaOfLength(5);
            settings.put(PROJECT_ID_SETTING.getConcreteSettingForNamespace(clientName).getKey(), projectId);
        } else {
            projectId = PROJECT_ID_SETTING.getDefault(Settings.EMPTY);
        }

        TimeValue connectTimeout;
        if (randomBoolean()) {
            connectTimeout = randomTimeout();
            settings.put(CONNECT_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), connectTimeout.getStringRep());
        } else {
            connectTimeout = CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY);
        }

        TimeValue readTimeout;
        if (randomBoolean()) {
            readTimeout = randomTimeout();
            settings.put(READ_TIMEOUT_SETTING.getConcreteSettingForNamespace(clientName).getKey(), readTimeout.getStringRep());
        } else {
            readTimeout = READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY);
        }

        String applicationName;
        if (randomBoolean()) {
            applicationName = randomAlphaOfLength(5);
            settings.put(APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName).getKey(), applicationName);
            deprecationWarnings.add(APPLICATION_NAME_SETTING.getConcreteSettingForNamespace(clientName));
        } else {
            applicationName = APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY);
        }

        return new GoogleCloudStorageClientSettings(
            credential,
            endpoint,
            projectId,
            connectTimeout,
            readTimeout,
            applicationName,
            new URI(""),
            new ProxySettings(Proxy.Type.DIRECT, null, 0, null, null)
        );
    }

    /** Generates a random GoogleCredential along with its corresponding Service Account file provided as a byte array **/
    private static Tuple<ServiceAccountCredentials, byte[]> randomCredential(final String clientName) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final ServiceAccountCredentials.Builder credentialBuilder = ServiceAccountCredentials.newBuilder();
        credentialBuilder.setClientId("id_" + clientName);
        credentialBuilder.setClientEmail(clientName);
        credentialBuilder.setProjectId("project_id_" + clientName);
        credentialBuilder.setPrivateKey(keyPair.getPrivate());
        credentialBuilder.setPrivateKeyId("private_key_id_" + clientName);
        credentialBuilder.setScopes(Collections.singleton(StorageScopes.DEVSTORAGE_FULL_CONTROL));
        final String encodedPrivateKey = Base64.getEncoder().encodeToString(keyPair.getPrivate().getEncoded());
        final String serviceAccount = "{\"type\":\"service_account\","
            + "\"project_id\":\"project_id_"
            + clientName
            + "\","
            + "\"private_key_id\":\"private_key_id_"
            + clientName
            + "\","
            + "\"private_key\":\"-----BEGIN PRIVATE KEY-----\\n"
            + encodedPrivateKey
            + "\\n-----END PRIVATE KEY-----\\n\","
            + "\"client_email\":\""
            + clientName
            + "\","
            + "\"client_id\":\"id_"
            + clientName
            + "\""
            + "}";
        return Tuple.tuple(credentialBuilder.build(), serviceAccount.getBytes(StandardCharsets.UTF_8));
    }

    private static TimeValue randomTimeout() {
        return randomFrom(TimeValue.MINUS_ONE, TimeValue.ZERO, TimeValue.parseTimeValue(randomPositiveTimeValue(), "test"));
    }

    private static void assertGoogleCredential(ServiceAccountCredentials expected, ServiceAccountCredentials actual) {
        if (expected != null) {
            assertEquals(expected.getServiceAccountUser(), actual.getServiceAccountUser());
            assertEquals(expected.getClientId(), actual.getClientId());
            assertEquals(expected.getClientEmail(), actual.getClientEmail());
            assertEquals(expected.getAccount(), actual.getAccount());
            assertEquals(expected.getProjectId(), actual.getProjectId());
            assertEquals(expected.getScopes(), actual.getScopes());
            assertEquals(expected.getPrivateKey(), actual.getPrivateKey());
            assertEquals(expected.getPrivateKeyId(), actual.getPrivateKeyId());
        } else {
            assertNull(actual);
        }
    }
}
