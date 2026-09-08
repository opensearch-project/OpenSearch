/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg.credentials;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Builds an {@link AwsCredentialsProvider} from {@link IcebergClientSettings} using the
 * same waterfall as open-source {@code repository-s3}:
 * <ol>
 *   <li>IRSA web-identity ({@code role_arn} + {@code identity_token_file}) →
 *       {@code StsWebIdentityTokenFileCredentialsProvider}</li>
 *   <li>IRSA assume-role ({@code role_arn} + {@code role_session_name}, no token file) →
 *       {@code StsAssumeRoleCredentialsProvider}</li>
 *   <li>Static keystore keys ({@code access_key}, {@code secret_key}, optional
 *       {@code session_token}) → {@code StaticCredentialsProvider}</li>
 *   <li>Fallback → {@code ContainerCredentialsProvider} (ECS) or
 *       {@code InstanceProfileCredentialsProvider} (EC2)</li>
 * </ol>
 * All resolving providers are wrapped in {@link PrivilegedCredentialsProvider}.
 */
public final class CredentialsBuilder {

    private static final Logger logger = LogManager.getLogger(CredentialsBuilder.class);

    private CredentialsBuilder() {}

    /**
     * Resolves the credentials provider for the given settings snapshot.
     *
     * @param settings parsed credential settings
     * @return a non-null privileged credentials provider
     */
    public static AwsCredentialsProvider build(IcebergClientSettings settings) {
        if (settings.getRoleArn() != null) {
            return buildIrsaProvider(settings);
        }
        if (settings.getStaticCredentials() != null) {
            logger.debug("Using static keystore credentials for catalog");
            return new PrivilegedCredentialsProvider(StaticCredentialsProvider.create(settings.getStaticCredentials()));
        }
        logger.debug("Using default container/instance-profile credentials for catalog");
        return new PrivilegedCredentialsProvider(buildDefaultChain());
    }

    private static AwsCredentialsProvider buildIrsaProvider(IcebergClientSettings settings) {
        final Region region = Region.of(settings.getRegion());

        // STS client used to back the IRSA provider. Seed it with static credentials if
        // present; otherwise DefaultCredentialsProvider (so a caller-supplied static
        // identity can assume the target role).
        StsClient stsClient = SocketAccess.doPrivileged(() -> {
            StsClientBuilder stsBuilder = StsClient.builder().region(region);
            if (settings.getStaticCredentials() != null) {
                AwsCredentials staticCreds = settings.getStaticCredentials();
                stsBuilder = stsBuilder.credentialsProvider(StaticCredentialsProvider.create(staticCreds));
            } else {
                stsBuilder = stsBuilder.credentialsProvider(DefaultCredentialsProvider.create());
            }
            return stsBuilder.build();
        });

        if (settings.getIdentityTokenFile() != null) {
            logger.debug("Using IRSA web-identity token file credentials for catalog");
            StsWebIdentityTokenFileCredentialsProvider provider = SocketAccess.doPrivileged(
                () -> StsWebIdentityTokenFileCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .roleArn(settings.getRoleArn())
                    .roleSessionName(settings.getRoleSessionName())
                    .webIdentityTokenFile(settings.getIdentityTokenFile())
                    .build()
            );
            return new PrivilegedCredentialsProvider(provider);
        }

        logger.debug("Using IRSA assume-role credentials for catalog");
        StsAssumeRoleCredentialsProvider provider = SocketAccess.doPrivileged(
            () -> StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(
                    AssumeRoleRequest.builder().roleArn(settings.getRoleArn()).roleSessionName(settings.getRoleSessionName()).build()
                )
                .build()
        );
        return new PrivilegedCredentialsProvider(provider);
    }

    private static AwsCredentialsProvider buildDefaultChain() {
        if (SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.getStringValue().isPresent()
            || SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.getStringValue().isPresent()) {
            return ContainerCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
        }
        return InstanceProfileCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
    }
}
