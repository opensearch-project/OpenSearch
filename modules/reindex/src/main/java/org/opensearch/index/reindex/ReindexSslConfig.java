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

package org.opensearch.index.reindex;

import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.SecureString;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.ssl.SslConfiguration;
import org.opensearch.common.ssl.SslConfigurationKeys;
import org.opensearch.common.ssl.SslConfigurationLoader;
import org.opensearch.env.Environment;
import org.opensearch.watcher.FileChangesListener;
import org.opensearch.watcher.FileWatcher;
import org.opensearch.watcher.ResourceWatcherService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.settings.Setting.listSetting;
import static org.opensearch.common.settings.Setting.simpleString;

/**
 * Loads "reindex.ssl.*" configuration from Settings, and makes the applicable configuration (trust manager / key manager / hostname
 * verification / cipher-suites) available for reindex-from-remote.
 */
class ReindexSslConfig {

    private static final Map<String, Setting<?>> SETTINGS = new HashMap<>();
    private static final Map<String, Setting<SecureString>> SECURE_SETTINGS = new HashMap<>();

    static {
        Setting.Property[] defaultProperties = new Setting.Property[] { Setting.Property.NodeScope, Setting.Property.Filtered };
        Setting.Property[] deprecatedProperties = new Setting.Property[] {
            Setting.Property.Deprecated,
            Setting.Property.NodeScope,
            Setting.Property.Filtered };
        for (String key : SslConfigurationKeys.getStringKeys()) {
            String settingName = "reindex.ssl." + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, simpleString(settingName, properties));
        }
        for (String key : SslConfigurationKeys.getListKeys()) {
            String settingName = "reindex.ssl." + key;
            final Setting.Property[] properties = SslConfigurationKeys.isDeprecated(key) ? deprecatedProperties : defaultProperties;
            SETTINGS.put(settingName, listSetting(settingName, Collections.emptyList(), Function.identity(), properties));
        }
        for (String key : SslConfigurationKeys.getSecureStringKeys()) {
            String settingName = "reindex.ssl." + key;
            SECURE_SETTINGS.put(settingName, SecureSetting.secureString(settingName, null));
        }
    }

    private final SslConfiguration configuration;
    private volatile SSLContext context;

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>();
        settings.addAll(SETTINGS.values());
        settings.addAll(SECURE_SETTINGS.values());
        return settings;
    }

    ReindexSslConfig(Settings settings, Environment environment, ResourceWatcherService resourceWatcher) {
        final SslConfigurationLoader loader = new SslConfigurationLoader("reindex.ssl.") {

            @Override
            protected String getSettingAsString(String key) {
                return settings.get(key);
            }

            @Override
            protected char[] getSecureSetting(String key) {
                final Setting<SecureString> setting = SECURE_SETTINGS.get(key);
                if (setting == null) {
                    throw new IllegalArgumentException("The secure setting [" + key + "] is not registered");
                }
                return setting.get(settings).getChars();
            }

            @Override
            protected List<String> getSettingAsList(String key) throws Exception {
                return settings.getAsList(key);
            }
        };
        configuration = loader.load(environment.configDir());
        reload();

        final FileChangesListener listener = new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileDeleted(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                ReindexSslConfig.this.reload();
            }
        };
        for (Path file : configuration.getDependentFiles()) {
            try {
                final FileWatcher watcher = new FileWatcher(file);
                watcher.addListener(listener);
                resourceWatcher.add(watcher, ResourceWatcherService.Frequency.HIGH);
            } catch (IOException e) {
                throw new UncheckedIOException("cannot watch file [" + file + "]", e);
            }
        }
    }

    private void reload() {
        this.context = configuration.createSslContext();
    }

    /**
     * Encapsulate the loaded SSL configuration as a HTTP-client {@link TlsStrategy}.
     * The returned strategy is immutable, but successive calls will return different objects that may have different
     * configurations if the underlying key/certificate files are modified.
     */
    TlsStrategy getStrategy() {
        final HostnameVerifier hostnameVerifier = configuration.getVerificationMode().isHostnameVerificationEnabled()
            ? new DefaultHostnameVerifier()
            : new NoopHostnameVerifier();

        final String[] protocols = configuration.getSupportedProtocols().toArray(new String[0]);
        final String[] cipherSuites = configuration.getCipherSuites().toArray(new String[0]);

        return ClientTlsStrategyBuilder.create()
            .setSslContext(context)
            .setHostnameVerifier(hostnameVerifier)
            .setCiphers(cipherSuites)
            .setTlsVersions(protocols)
            // See https://issues.apache.org/jira/browse/HTTPCLIENT-2219
            .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
                @Override
                public TlsDetails create(final SSLEngine sslEngine) {
                    return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
                }
            })
            .build();

    }
}
