/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.crypto.kms;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;

import java.util.function.Supplier;

/**
 * Creates credential providers based on the provided configuration.
 */
public class CredentialProviderFactory {

    /**
     * Credential provider for EC2/ECS container
     */
    static class PrivilegedInstanceProfileCredentialsProvider implements AwsCredentialsProvider {
        private final AwsCredentialsProvider credentials;

        private PrivilegedInstanceProfileCredentialsProvider() {
            this.credentials = initializeProvider();
        }

        private AwsCredentialsProvider initializeProvider() {
            if (SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI.getStringValue().isPresent()
                || SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.getStringValue().isPresent()) {

                return ContainerCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
            }
            // InstanceProfileCredentialsProvider as last item of chain
            return InstanceProfileCredentialsProvider.builder().asyncCredentialUpdateEnabled(true).build();
        }

        @Override
        public AwsCredentials resolveCredentials() {
            return SocketAccess.doPrivileged(credentials::resolveCredentials);
        }
    }

    /**
     * Creates a credential provider based on the provided configuration.
     * @param staticCredsSupplier Static credentials are used in case supplier returns a non-null instance.
     * @return Credential provider instance.
     */
    public AwsCredentialsProvider createAwsCredentialsProvider(Supplier<AwsCredentials> staticCredsSupplier) {
        AwsCredentials awsCredentials = staticCredsSupplier.get();
        if (awsCredentials != null) {
            return StaticCredentialsProvider.create(awsCredentials);
        }

        // Add other credential providers here
        return new PrivilegedInstanceProfileCredentialsProvider();
    }
}
