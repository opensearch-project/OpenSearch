/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg.credentials;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;
import org.opensearch.plugin.catalog.iceberg.IcebergCatalogRepository;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Typed snapshot of all credential-related settings for the Iceberg catalog.
 * <p>
 * Pulls:
 * <ul>
 *   <li>IRSA / assume-role inputs ({@code role_arn}, {@code role_session_name},
 *       {@code identity_token_file}) from the repository's node settings.</li>
 *   <li>Static credentials ({@code access_key}, {@code secret_key},
 *       {@code session_token}) from the cluster keystore under the
 *       {@code catalog.credentials.} prefix.</li>
 *   <li>Region from the repository's node settings.</li>
 * </ul>
 * Relative {@code identity_token_file} paths are resolved against {@link Environment#configDir()}.
 */
public final class IcebergClientSettings {

    /** Prefix for credentials pulled from the cluster keystore. */
    public static final String KEYSTORE_PREFIX = "catalog.credentials.";

    /** Keystore: AWS access key id. */
    public static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString(KEYSTORE_PREFIX + "access_key", null);

    /** Keystore: AWS secret access key. */
    public static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString(KEYSTORE_PREFIX + "secret_key", null);

    /** Keystore: AWS session token (used with temporary credentials). */
    public static final Setting<SecureString> SESSION_TOKEN_SETTING = SecureSetting.secureString(KEYSTORE_PREFIX + "session_token", null);

    private final String region;
    private final AwsCredentials staticCredentials; // may be null
    private final String roleArn;                   // may be null
    private final String roleSessionName;           // may be null
    private final Path identityTokenFile;           // may be null

    private IcebergClientSettings(
        String region,
        AwsCredentials staticCredentials,
        String roleArn,
        String roleSessionName,
        Path identityTokenFile
    ) {
        this.region = region;
        this.staticCredentials = staticCredentials;
        this.roleArn = roleArn;
        this.roleSessionName = roleSessionName;
        this.identityTokenFile = identityTokenFile;
    }

    /**
     * Builds a settings snapshot from the given repository and environment.
     *
     * @param repository  catalog repository (source of region + IRSA inputs)
     * @param environment node environment (source of keystore + config dir)
     * @return a new typed settings snapshot; never null
     */
    public static IcebergClientSettings load(IcebergCatalogRepository repository, Environment environment) {
        String region = repository.getRegion();
        AwsCredentials staticCreds = loadStaticCredentials(environment.settings());
        String roleArn = nullIfEmpty(repository.getRoleArn());
        String roleSessionName = nullIfEmpty(repository.getRoleSessionName());
        Path tokenFile = resolveIdentityTokenFile(repository.getIdentityTokenFile(), environment);
        return new IcebergClientSettings(region, staticCreds, roleArn, roleSessionName, tokenFile);
    }

    private static AwsCredentials loadStaticCredentials(Settings nodeSettings) {
        try (
            SecureString accessKey = ACCESS_KEY_SETTING.get(nodeSettings);
            SecureString secretKey = SECRET_KEY_SETTING.get(nodeSettings);
            SecureString sessionToken = SESSION_TOKEN_SETTING.get(nodeSettings)
        ) {
            if (accessKey.length() == 0 && secretKey.length() == 0) {
                if (sessionToken.length() > 0) {
                    throw new IllegalArgumentException(
                        "["
                            + SESSION_TOKEN_SETTING.getKey()
                            + "] requires both ["
                            + ACCESS_KEY_SETTING.getKey()
                            + "] and ["
                            + SECRET_KEY_SETTING.getKey()
                            + "]"
                    );
                }
                return null;
            }
            if (accessKey.length() == 0) {
                throw new IllegalArgumentException(
                    "[" + ACCESS_KEY_SETTING.getKey() + "] is required when [" + SECRET_KEY_SETTING.getKey() + "] is set"
                );
            }
            if (secretKey.length() == 0) {
                throw new IllegalArgumentException(
                    "[" + SECRET_KEY_SETTING.getKey() + "] is required when [" + ACCESS_KEY_SETTING.getKey() + "] is set"
                );
            }
            if (sessionToken.length() > 0) {
                return AwsSessionCredentials.create(accessKey.toString(), secretKey.toString(), sessionToken.toString());
            }
            return AwsBasicCredentials.create(accessKey.toString(), secretKey.toString());
        }
    }

    @SuppressForbidden(reason = "relative token file path is resolved against Environment#configDir()")
    private static Path resolveIdentityTokenFile(String configured, Environment environment) {
        if (Strings.isNullOrEmpty(configured)) {
            return null;
        }
        Path p = Paths.get(configured);
        if (!p.isAbsolute()) {
            p = environment.configDir().resolve(configured);
        }
        return p;
    }

    private static String nullIfEmpty(String s) {
        return Strings.isNullOrEmpty(s) ? null : s;
    }

    /** Returns the configured AWS region. Never null or empty. */
    public String getRegion() {
        return region;
    }

    /** Returns the static credentials from the keystore, or {@code null} if unset. */
    public AwsCredentials getStaticCredentials() {
        return staticCredentials;
    }

    /** Returns the configured role ARN to assume, or {@code null} if unset. */
    public String getRoleArn() {
        return roleArn;
    }

    /** Returns the configured STS role session name, or {@code null} if unset. */
    public String getRoleSessionName() {
        return roleSessionName;
    }

    /** Returns the resolved web-identity token file, or {@code null} if unset. */
    public Path getIdentityTokenFile() {
        return identityTokenFile;
    }
}
