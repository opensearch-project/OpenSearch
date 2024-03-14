/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import org.opensearch.SpecialPermission;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.crypto.MasterKeyProvider;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.CryptoKeyProviderPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ReloadablePlugin;

import java.util.Arrays;
import java.util.List;

/**
 * AWS KMS based crypto key provider plugin.
 */
public class CryptoKmsPlugin extends Plugin implements CryptoKeyProviderPlugin, ReloadablePlugin {
    private static final String PROVIDER_NAME = "aws-kms";

    static {
        SpecialPermission.check();
    }

    private final Settings settings;
    // protected for testing
    protected final KmsService kmsService;

    public CryptoKmsPlugin(Settings settings) {
        this(settings, new KmsService());
    }

    protected CryptoKmsPlugin(Settings settings, KmsService kmsService) {
        this.settings = settings;
        this.kmsService = kmsService;
        // eagerly load client settings when secure settings are accessible
        reload(settings);
    }

    @Override
    public MasterKeyProvider createKeyProvider(CryptoMetadata cryptoMetadata) {
        return kmsService.createMasterKeyProvider(cryptoMetadata);
    }

    @Override
    public String type() {
        return PROVIDER_NAME;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            KmsClientSettings.ACCESS_KEY_SETTING,
            KmsClientSettings.SECRET_KEY_SETTING,
            KmsClientSettings.SESSION_TOKEN_SETTING,
            KmsClientSettings.ENDPOINT_SETTING,
            KmsClientSettings.REGION_SETTING,
            KmsClientSettings.PROXY_HOST_SETTING,
            KmsClientSettings.PROXY_PORT_SETTING,
            KmsClientSettings.PROXY_USERNAME_SETTING,
            KmsClientSettings.PROXY_PASSWORD_SETTING,
            KmsClientSettings.READ_TIMEOUT_SETTING,
            KmsService.ENC_CTX_SETTING,
            KmsService.KEY_ARN_SETTING
        );
    }

    @Override
    public void reload(Settings settings) {
        // secure settings should be readable
        final KmsClientSettings clientSettings = KmsClientSettings.getClientSettings(settings);
        kmsService.refreshAndClearCache(clientSettings);
    }

    @Override
    public void close() {
        kmsService.close();
    }
}
