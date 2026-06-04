/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

public class CryptoKmsClientSettingsTests extends AbstractAwsTestCase {

    public void testNondefaultClientCreatedBySettingItsSettings() {
        final KmsClientSettings settings = KmsClientSettings.getClientSettings(
            Settings.builder().put("kms.endpoint", "custom_endpoint").build()
        );

        assertEquals(settings.endpoint, "custom_endpoint");
        // Check if defaults are still present
        assertNotNull(settings.proxyHost);
    }

    public void testRejectionOfLoneAccessKey() throws IOException {
        try (final MockSecureSettings secureSettings = new MockSecureSettings()) {
            secureSettings.setString("kms.access_key", "aws_secret");
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> KmsClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            );
            assertTrue(e.getMessage().contains("Setting [kms.access_key] is set but [kms.secret_key] is not"));
        }
    }

    public void testRejectionOfLoneSecretKey() throws IOException {
        try (final MockSecureSettings secureSettings = new MockSecureSettings()) {
            secureSettings.setString("kms.secret_key", "aws_key");
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> KmsClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            );
            assertTrue(e.getMessage().contains("Setting [kms.secret_key] is set but [kms.access_key] is not"));
        }
    }

    public void testRejectionOfLoneSessionToken() throws IOException {
        try (final MockSecureSettings secureSettings = new MockSecureSettings()) {
            secureSettings.setString("kms.session_token", "aws_session_token");
            final SettingsException e = expectThrows(
                SettingsException.class,
                () -> KmsClientSettings.getClientSettings(Settings.builder().setSecureSettings(secureSettings).build())
            );
            assertTrue(e.getMessage().contains("Setting [kms.session_token] is set but [kms.access_key] and [kms.secret_key] are not"));
        }
    }

    public void testDefaultEndpoint() {
        KmsClientSettings baseSettings = KmsClientSettings.getClientSettings(Settings.EMPTY);
        assertEquals(baseSettings.endpoint, "");
    }

    public void testDefaultRegion() {
        final Settings settings = Settings.builder().build();
        KmsClientSettings baseSettings = KmsClientSettings.getClientSettings(settings);
        assertEquals(baseSettings.region, "");
    }

    public void testSpecificRegion() {
        final Settings settings = Settings.builder().put(KmsClientSettings.REGION_SETTING.getKey(), "us-west-2").build();
        KmsClientSettings baseSettings = KmsClientSettings.getClientSettings(settings);
        assertEquals(baseSettings.region, Region.US_WEST_2.toString());
    }

    public void testSpecificEndpoint() {
        final Settings settings = Settings.builder().put(KmsClientSettings.ENDPOINT_SETTING.getKey(), "kms.endpoint").build();
        KmsClientSettings baseSettings = KmsClientSettings.getClientSettings(settings);
        assertEquals(baseSettings.endpoint, "kms.endpoint");
    }

    public void testOverrideWithPrefixedMetadataSettings() {
        overrideWithMetadataSettings("kms.");
    }

    public void testOverrideWithNoPrefixMetadataSettings() {
        overrideWithMetadataSettings("");
    }

    public void overrideWithMetadataSettings(String prefix) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        String accessKey = "access_key", secretKey = "secret_key", sessionToken = "session_token";
        secureSettings.setString("kms.access_key", accessKey);
        secureSettings.setString("kms.secret_key", secretKey);
        secureSettings.setString("kms.session_token", sessionToken);
        final KmsClientSettings baseSettings = KmsClientSettings.getClientSettings(
            Settings.builder().setSecureSettings(secureSettings).build()
        );

        {
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(Settings.EMPTY);
            assertEquals(refinedSettings, baseSettings);
        }

        {
            final String endpoint = "some.host";
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(
                Settings.builder().put(prefix + "endpoint", endpoint).build()
            );
            assertEquals(refinedSettings.endpoint, endpoint);
            validateCredsAreStillSame(refinedSettings, accessKey, secretKey, sessionToken);
        }

        {
            String region = "eu-west-1";
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(
                Settings.builder().put(prefix + "region", region).build()
            );
            assertEquals(refinedSettings.region, region);
            validateCredsAreStillSame(refinedSettings, accessKey, secretKey, sessionToken);
        }

        {
            String proxyHost = "proxy-host";
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(
                Settings.builder().put(prefix + "proxy.host", proxyHost).build()
            );
            assertEquals(refinedSettings.proxyHost, proxyHost);
            validateCredsAreStillSame(refinedSettings, accessKey, secretKey, sessionToken);
        }

        {
            int proxyPort = 70;
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(
                Settings.builder().put(prefix + "proxy.port", proxyPort).build()
            );
            assertEquals(refinedSettings.proxyPort, proxyPort);
            validateCredsAreStillSame(refinedSettings, accessKey, secretKey, sessionToken);
        }

        {
            TimeValue readTimeout = TimeValue.timeValueMillis(5000);
            final KmsClientSettings refinedSettings = baseSettings.getMetadataSettings(
                Settings.builder().put(prefix + "read_timeout", readTimeout).build()
            );
            assertEquals(refinedSettings.readTimeoutMillis, readTimeout.getMillis());
            validateCredsAreStillSame(refinedSettings, accessKey, secretKey, sessionToken);
        }
    }

    private void validateCredsAreStillSame(KmsClientSettings refinedSettings, String accessKey, String secretKey, String sessionToken) {
        AwsSessionCredentials credentials = (AwsSessionCredentials) refinedSettings.credentials;
        assertEquals(credentials.accessKeyId(), accessKey);
        assertEquals(credentials.secretAccessKey(), secretKey);
        assertEquals(credentials.sessionToken(), sessionToken);
    }
}
