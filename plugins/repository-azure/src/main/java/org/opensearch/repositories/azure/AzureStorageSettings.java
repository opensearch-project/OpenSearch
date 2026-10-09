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

package org.opensearch.repositories.azure;

import com.azure.core.util.logging.ClientLogger;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.implementation.CredentialBuilderBaseHelper;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.common.implementation.Constants;
import com.azure.storage.common.implementation.connectionstring.StorageConnectionString;
import com.azure.storage.common.implementation.connectionstring.StorageEndpoint;
import org.opensearch.common.Nullable;
import org.opensearch.common.TriFunction;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.AffixSetting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

final class AzureStorageSettings {

    // prefix for azure client settings
    private static final String AZURE_CLIENT_PREFIX_KEY = "azure.client.";
    private static final ByteSizeValue UNSET_TRANSFER_SIZE = new ByteSizeValue(-1, ByteSizeUnit.BYTES);
    private static final ByteSizeValue MAX_READ_BLOCK_SIZE = new ByteSizeValue(Integer.MAX_VALUE, ByteSizeUnit.BYTES);
    private static final ByteSizeValue MAX_SINGLE_UPLOAD_SIZE = new ByteSizeValue(
        BlockBlobAsyncClient.MAX_UPLOAD_BLOB_BYTES_LONG,
        ByteSizeUnit.BYTES
    );

    /** Azure account name */
    public static final AffixSetting<SecureString> ACCOUNT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "account",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure key */
    public static final AffixSetting<SecureString> KEY_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "key",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure SAS token */
    public static final AffixSetting<SecureString> SAS_TOKEN_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "sas_token",
        key -> SecureSetting.secureString(key, null)
    );

    /** Azure token credentials such as Managed Identity */
    public static final AffixSetting<String> TOKEN_CREDENTIAL_TYPE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "token_credential_type",
        key -> Setting.simpleString(key, value -> {
            if (Strings.hasText(value) == true) {
                TokenCredentialType.valueOfType(value);
            }
        }, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    /** max_retries: Number of retries in case of Azure errors. Defaults to 3 (RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT). */
    public static final AffixSetting<Integer> MAX_RETRIES_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "max_retries",
        (key) -> Setting.intSetting(key, 3, Setting.Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );
    /**
     * Azure endpoint suffix. Default to core.windows.net (CloudStorageAccount.DEFAULT_DNS).
     */
    public static final AffixSetting<String> ENDPOINT_SUFFIX_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "endpoint_suffix",
        key -> Setting.simpleString(key, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    // The overall operation timeout
    public static final AffixSetting<TimeValue> TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueMinutes(-1), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_CONNECT_TIMEOUT
    public static final AffixSetting<TimeValue> CONNECT_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "connect.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(10), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_WRITE_TIMEOUT
    public static final AffixSetting<TimeValue> WRITE_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "write.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_READ_TIMEOUT
    public static final AffixSetting<TimeValue> READ_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "read.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    // See please NettyAsyncHttpClientBuilder#DEFAULT_RESPONSE_TIMEOUT
    public static final AffixSetting<TimeValue> RESPONSE_TIMEOUT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "response.timeout",
        (key) -> Setting.timeSetting(key, TimeValue.timeValueSeconds(60), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    /** Read block size. The Azure SDK default is used when unset. */
    public static final AffixSetting<ByteSizeValue> READ_BLOCK_SIZE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "read.block_size",
        key -> optionalTransferSizeSetting(key, MAX_READ_BLOCK_SIZE),
        () -> ACCOUNT_SETTING
    );

    /** Upload block size. The Azure SDK default is used when unset. */
    public static final AffixSetting<ByteSizeValue> WRITE_BLOCK_SIZE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "write.block_size",
        key -> optionalTransferSizeSetting(key, AzureStorageService.MAX_CHUNK_SIZE),
        () -> ACCOUNT_SETTING
    );

    /** Largest upload sent as one Put Blob request. The Azure SDK default is used when unset. */
    public static final AffixSetting<ByteSizeValue> MAX_SINGLE_UPLOAD_SIZE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "write.max_single_upload_size",
        key -> optionalTransferSizeSetting(key, MAX_SINGLE_UPLOAD_SIZE),
        () -> ACCOUNT_SETTING
    );

    /** Maximum concurrent requests per upload. The Azure SDK default is used when unset. */
    public static final AffixSetting<Integer> WRITE_CONCURRENCY_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "write.max_concurrency",
        key -> Setting.intSetting(key, -1, -1, value -> {
            if (value == 0) {
                throw new IllegalArgumentException("setting [" + key + "] must be -1 or at least 1");
            }
        }, Property.NodeScope),
        () -> ACCOUNT_SETTING
    );

    /** The type of the proxy to connect to azure through. Can be direct (no proxy, default), http or socks */
    public static final AffixSetting<ProxySettings.ProxyType> PROXY_TYPE_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.type",
        (key) -> new Setting<>(key, "direct", s -> ProxySettings.ProxyType.valueOf(s.toUpperCase(Locale.ROOT)), Property.NodeScope),
        () -> ACCOUNT_SETTING,
        () -> KEY_SETTING
    );

    /** The host name of a proxy to connect to azure through. */
    public static final AffixSetting<String> PROXY_HOST_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.host",
        (key) -> Setting.simpleString(key, Property.NodeScope),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING
    );

    /** The port of a proxy to connect to azure through. */
    public static final AffixSetting<Integer> PROXY_PORT_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.port",
        (key) -> Setting.intSetting(key, 0, 0, 65535, Setting.Property.NodeScope),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The username of a proxy to connect */
    static final AffixSetting<SecureString> PROXY_USERNAME_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.username",
        key -> SecureSetting.secureString(key, null),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING
    );

    /** The password of a proxy to connect */
    static final AffixSetting<SecureString> PROXY_PASSWORD_SETTING = Setting.affixKeySetting(
        AZURE_CLIENT_PREFIX_KEY,
        "proxy.password",
        key -> SecureSetting.secureString(key, null),
        () -> KEY_SETTING,
        () -> ACCOUNT_SETTING,
        () -> PROXY_TYPE_SETTING,
        () -> PROXY_HOST_SETTING,
        () -> PROXY_USERNAME_SETTING
    );

    private final String account;
    private final String tokenCredentialType;
    private final TriFunction<BlobServiceClientBuilder, ExecutorService, ClientLogger, BlobServiceClientBuilder> clientBuilder;
    private final Function<ClientLogger, StorageEndpoint> endpointBuilder;
    private final String endpointSuffix;
    private final TimeValue timeout;
    private final int maxRetries;
    private final LocationMode locationMode;
    private final TimeValue connectTimeout;
    private final TimeValue writeTimeout;
    private final TimeValue readTimeout;
    private final TimeValue responseTimeout;
    private final ByteSizeValue readBlockSize;
    private final ByteSizeValue writeBlockSize;
    private final ByteSizeValue maxSingleUploadSize;
    private final int writeConcurrency;
    private final ProxySettings proxySettings;

    // copy-constructor
    private AzureStorageSettings(
        String account,
        String tokenCredentialType,
        TriFunction<BlobServiceClientBuilder, ExecutorService, ClientLogger, BlobServiceClientBuilder> clientBuilder,
        Function<ClientLogger, StorageEndpoint> endpointBuilder,
        String endpointSuffix,
        TimeValue timeout,
        int maxRetries,
        LocationMode locationMode,
        TimeValue connectTimeout,
        TimeValue writeTimeout,
        TimeValue readTimeout,
        TimeValue responseTimeout,
        ByteSizeValue readBlockSize,
        ByteSizeValue writeBlockSize,
        ByteSizeValue maxSingleUploadSize,
        int writeConcurrency,
        ProxySettings proxySettings
    ) {
        this.account = account;
        this.tokenCredentialType = tokenCredentialType;
        this.clientBuilder = clientBuilder;
        this.endpointBuilder = endpointBuilder;
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.locationMode = locationMode;
        this.connectTimeout = connectTimeout;
        this.writeTimeout = writeTimeout;
        this.readTimeout = readTimeout;
        this.responseTimeout = responseTimeout;
        this.readBlockSize = readBlockSize;
        this.writeBlockSize = writeBlockSize;
        this.maxSingleUploadSize = maxSingleUploadSize;
        this.writeConcurrency = writeConcurrency;
        this.proxySettings = proxySettings;
    }

    private AzureStorageSettings(
        String account,
        String key,
        String sasToken,
        String tokenCredentialType,
        String endpointSuffix,
        TimeValue timeout,
        int maxRetries,
        TimeValue connectTimeout,
        TimeValue writeTimeout,
        TimeValue readTimeout,
        TimeValue responseTimeout,
        ByteSizeValue readBlockSize,
        ByteSizeValue writeBlockSize,
        ByteSizeValue maxSingleUploadSize,
        int writeConcurrency,
        ProxySettings proxySettings
    ) {
        this.account = account;
        this.tokenCredentialType = tokenCredentialType;
        if (Strings.hasText(tokenCredentialType) == true) {
            this.endpointBuilder = (logger) -> {
                String tokenCredentialEndpointSuffix = endpointSuffix;
                if (Strings.hasText(tokenCredentialEndpointSuffix) == false) {
                    // Default to "core.windows.net".
                    tokenCredentialEndpointSuffix = Constants.ConnectionStringConstants.DEFAULT_DNS;
                }
                final URI primaryBlobEndpoint = URI.create("https://" + account + ".blob." + tokenCredentialEndpointSuffix);
                final URI secondaryBlobEndpoint = URI.create("https://" + account + "-secondary.blob." + tokenCredentialEndpointSuffix);
                return new StorageEndpoint(primaryBlobEndpoint, secondaryBlobEndpoint);
            };

            this.clientBuilder = (builder, executor, logger) -> builder.credential(new ManagedIdentityCredentialBuilder() {
                @Override
                public ManagedIdentityCredential build() {
                    // Use the privileged executor with IdentityClient instance
                    CredentialBuilderBaseHelper.getClientOptions(this).setExecutorService(executor);
                    return super.build();
                }
            }.build()).endpoint(endpointBuilder.apply(logger).getPrimaryUri());
        } else {
            final String connectString = buildConnectString(account, key, sasToken, endpointSuffix);

            this.endpointBuilder = (logger) -> {
                final StorageConnectionString storageConnectionString = StorageConnectionString.create(connectString, logger);
                return storageConnectionString.getBlobEndpoint();
            };

            this.clientBuilder = (builder, executor, logger) -> builder.connectionString(connectString);
        }
        this.endpointSuffix = endpointSuffix;
        this.timeout = timeout;
        this.maxRetries = maxRetries;
        this.locationMode = LocationMode.PRIMARY_ONLY;
        this.connectTimeout = connectTimeout;
        this.writeTimeout = writeTimeout;
        this.readTimeout = readTimeout;
        this.responseTimeout = responseTimeout;
        this.readBlockSize = readBlockSize;
        this.writeBlockSize = writeBlockSize;
        this.maxSingleUploadSize = maxSingleUploadSize;
        this.writeConcurrency = writeConcurrency;
        this.proxySettings = proxySettings;
    }

    public String getTokenCredentialType() {
        return tokenCredentialType;
    }

    public StorageEndpoint getStorageEndpoint(ClientLogger logger) {
        return endpointBuilder.apply(logger);
    }

    public String getEndpointSuffix() {
        return endpointSuffix;
    }

    public TimeValue getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public ProxySettings getProxySettings() {
        return proxySettings;
    }

    private static String buildConnectString(String account, @Nullable String key, @Nullable String sasToken, String endpointSuffix) {
        final boolean hasSasToken = Strings.hasText(sasToken);
        final boolean hasKey = Strings.hasText(key);
        if (hasSasToken == false && hasKey == false) {
            throw new SettingsException("Neither a secret key nor a shared access token was set.");
        }
        if (hasSasToken && hasKey) {
            throw new SettingsException("Both a secret as well as a shared access token were set.");
        }
        final StringBuilder connectionStringBuilder = new StringBuilder();
        connectionStringBuilder.append("DefaultEndpointsProtocol=https").append(";AccountName=").append(account);
        if (hasKey) {
            connectionStringBuilder.append(";AccountKey=").append(key);
        } else {
            connectionStringBuilder.append(";SharedAccessSignature=").append(sasToken);
        }
        if (Strings.hasText(endpointSuffix)) {
            connectionStringBuilder.append(";EndpointSuffix=").append(endpointSuffix);
        }
        return connectionStringBuilder.toString();
    }

    public LocationMode getLocationMode() {
        return locationMode;
    }

    public TimeValue getConnectTimeout() {
        return connectTimeout;
    }

    public TimeValue getWriteTimeout() {
        return writeTimeout;
    }

    public TimeValue getReadTimeout() {
        return readTimeout;
    }

    public TimeValue getResponseTimeout() {
        return responseTimeout;
    }

    public ByteSizeValue getReadBlockSize() {
        return readBlockSize;
    }

    public ByteSizeValue getWriteBlockSize() {
        return writeBlockSize;
    }

    public ByteSizeValue getMaxSingleUploadSize() {
        return maxSingleUploadSize;
    }

    public int getWriteConcurrency() {
        return writeConcurrency;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AzureStorageSettings{");
        sb.append("account='").append(account).append('\'');
        sb.append(", timeout=").append(timeout);
        sb.append(", tokenCredentialType=").append(tokenCredentialType).append('\'');
        sb.append(", endpointSuffix='").append(endpointSuffix).append('\'');
        sb.append(", maxRetries=").append(maxRetries);
        sb.append(", proxySettings=").append(proxySettings != ProxySettings.NO_PROXY_SETTINGS ? "PROXY_SET" : "PROXY_NOT_SET");
        sb.append(", locationMode='").append(locationMode).append('\'');
        sb.append(", connectTimeout='").append(connectTimeout).append('\'');
        sb.append(", writeTimeout='").append(writeTimeout).append('\'');
        sb.append(", readTimeout='").append(readTimeout).append('\'');
        sb.append(", responseTimeout='").append(responseTimeout).append('\'');
        sb.append(", readBlockSize='").append(readBlockSize).append('\'');
        sb.append(", writeBlockSize='").append(writeBlockSize).append('\'');
        sb.append(", maxSingleUploadSize='").append(maxSingleUploadSize).append('\'');
        sb.append(", writeConcurrency='").append(writeConcurrency).append('\'');
        sb.append('}');
        return sb.toString();
    }

    private static Setting<ByteSizeValue> optionalTransferSizeSetting(String key, ByteSizeValue maxValue) {
        return new Setting<>(
            key,
            UNSET_TRANSFER_SIZE.getStringRep(),
            new Setting.ByteSizeValueParser(UNSET_TRANSFER_SIZE, maxValue, key),
            value -> {
                if (value.getBytes() == 0L) {
                    throw new IllegalArgumentException("setting [" + key + "] must be -1 or at least 1b");
                }
            },
            Property.NodeScope
        );
    }

    /**
     * Parse and read all settings available under the azure.client.* namespace
     * @param settings settings to parse
     * @return All the named configurations
     */
    public static Map<String, AzureStorageSettings> load(Settings settings) {
        // Get the list of existing named configurations
        final Map<String, AzureStorageSettings> storageSettings = new HashMap<>();
        for (final String clientName : ACCOUNT_SETTING.getNamespaces(settings)) {
            storageSettings.put(clientName, getClientSettings(settings, clientName));
        }
        if (false == storageSettings.containsKey("default") && false == storageSettings.isEmpty()) {
            // in case no setting named "default" has been set, let's define our "default"
            // as the first named config we get
            final AzureStorageSettings defaultSettings = storageSettings.values().iterator().next();
            storageSettings.put("default", defaultSettings);
        }
        assert storageSettings.containsKey("default") || storageSettings.isEmpty() : "always have 'default' if any";
        return Collections.unmodifiableMap(storageSettings);
    }

    // pkg private for tests
    /** Parse settings for a single client. */
    private static AzureStorageSettings getClientSettings(Settings settings, String clientName) {
        try (
            SecureString account = getConfigValue(settings, clientName, ACCOUNT_SETTING);
            SecureString key = getConfigValue(settings, clientName, KEY_SETTING);
            SecureString sasToken = getConfigValue(settings, clientName, SAS_TOKEN_SETTING)
        ) {
            return new AzureStorageSettings(
                account.toString(),
                key.toString(),
                sasToken.toString(),
                getValue(settings, clientName, TOKEN_CREDENTIAL_TYPE_SETTING),
                getValue(settings, clientName, ENDPOINT_SUFFIX_SETTING),
                getValue(settings, clientName, TIMEOUT_SETTING),
                getValue(settings, clientName, MAX_RETRIES_SETTING),
                getValue(settings, clientName, CONNECT_TIMEOUT_SETTING),
                getValue(settings, clientName, WRITE_TIMEOUT_SETTING),
                getValue(settings, clientName, READ_TIMEOUT_SETTING),
                getValue(settings, clientName, RESPONSE_TIMEOUT_SETTING),
                getValue(settings, clientName, READ_BLOCK_SIZE_SETTING),
                getValue(settings, clientName, WRITE_BLOCK_SIZE_SETTING),
                getValue(settings, clientName, MAX_SINGLE_UPLOAD_SIZE_SETTING),
                getValue(settings, clientName, WRITE_CONCURRENCY_SETTING),
                validateAndCreateProxySettings(settings, clientName)
            );
        }
    }

    static ProxySettings validateAndCreateProxySettings(final Settings settings, final String clientName) {
        final ProxySettings.ProxyType proxyType = getConfigValue(settings, clientName, PROXY_TYPE_SETTING);
        final String proxyHost = getConfigValue(settings, clientName, PROXY_HOST_SETTING);
        final int proxyPort = getConfigValue(settings, clientName, PROXY_PORT_SETTING);
        final SecureString proxyUserName = getConfigValue(settings, clientName, PROXY_USERNAME_SETTING);
        final SecureString proxyPassword = getConfigValue(settings, clientName, PROXY_PASSWORD_SETTING);
        // Validate proxy settings
        if (proxyType == ProxySettings.ProxyType.DIRECT
            && (proxyPort != 0 || Strings.hasText(proxyHost) || Strings.hasText(proxyUserName) || Strings.hasText(proxyPassword))) {
            throw new SettingsException("Azure proxy port or host or username or password have been set but proxy type is not defined.");
        }
        if (proxyType != ProxySettings.ProxyType.DIRECT && (proxyPort == 0 || Strings.isEmpty(proxyHost))) {
            throw new SettingsException("Azure proxy type has been set but proxy host or port is not defined.");
        }

        if (proxyType == ProxySettings.ProxyType.DIRECT) {
            return ProxySettings.NO_PROXY_SETTINGS;
        }

        try {
            final InetAddress proxyHostAddress = InetAddress.getByName(proxyHost);
            return new ProxySettings(proxyType, proxyHostAddress, proxyPort, proxyUserName.toString(), proxyPassword.toString());
        } catch (final UnknownHostException e) {
            throw new SettingsException("Azure proxy host is unknown.", e);
        }
    }

    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }

    private static <T> T getValue(Settings settings, String groupName, Setting<T> setting) {
        final Setting.AffixKey k = (Setting.AffixKey) setting.getRawKey();
        final String fullKey = k.toConcreteKey(groupName).toString();
        return setting.getConcreteSetting(fullKey).get(settings);
    }

    static Map<String, AzureStorageSettings> overrideLocationMode(
        Map<String, AzureStorageSettings> clientsSettings,
        LocationMode locationMode
    ) {
        final MapBuilder<String, AzureStorageSettings> mapBuilder = new MapBuilder<>();
        for (final Map.Entry<String, AzureStorageSettings> entry : clientsSettings.entrySet()) {
            mapBuilder.put(entry.getKey(), entry.getValue().withLocationMode(locationMode));
        }
        return mapBuilder.immutableMap();
    }

    AzureStorageSettings withLocationMode(LocationMode locationMode) {
        return new AzureStorageSettings(
            account,
            tokenCredentialType,
            clientBuilder,
            endpointBuilder,
            endpointSuffix,
            timeout,
            maxRetries,
            locationMode,
            connectTimeout,
            writeTimeout,
            readTimeout,
            responseTimeout,
            readBlockSize,
            writeBlockSize,
            maxSingleUploadSize,
            writeConcurrency,
            proxySettings
        );
    }

    public BlobServiceClientBuilder configure(BlobServiceClientBuilder builder, ExecutorService executor, ClientLogger logger) {
        return clientBuilder.apply(builder, executor, logger);
    }
}
