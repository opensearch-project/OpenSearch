/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for AWS SDK endpoint resolution behavior for S3 bucket ARNs (Outposts / Access Points),
 * and OpenSearch repository-s3 behavior around endpoint overrides.
 *
 * These tests are intentionally "no-network": they capture the resolved request URI using an
 * ExecutionInterceptor and abort the request via a failing HTTP client.
 */
public class S3ArnEndpointResolutionTests extends AbstractS3RepositoryTestCase {

    // ---- Helpers ----

    private static final class CapturingInterceptor implements ExecutionInterceptor {
        private final AtomicReference<URI> captured = new AtomicReference<>();

        @Override
        public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            captured.set(context.httpRequest().getUri());
        }

        URI capturedUri() {
            return captured.get();
        }
    }

    private static SdkHttpClient failingHttpClient() {
        return new SdkHttpClient() {
            @Override
            public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
                return new ExecutableHttpRequest() {
                    @Override
                    public HttpExecuteResponse call() {
                        throw new RuntimeException("stop after endpoint resolution");
                    }

                    @Override
                    public void abort() {
                        // no-op
                    }
                };
            }

            @Override
            public void close() {
                // no-op
            }
        };
    }

    private static void issueNoNetworkRequest(S3Client client, String bucket) {
        try {
            client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
            fail("Expected to fail due to no-network http client");
        } catch (RuntimeException e) {
            // expected
        }
    }

    // ---- SDK contract tests ----

    public void testSdkResolvesOutpostsEndpointFromBucketArn() {
        // Shape matters more than reality for endpoint logic; account/outpost IDs can be dummy.
        final String bucketArn = "arn:aws:s3-outposts:us-east-1:111122223333:outpost/op-0123456789abcdef/accesspoint/my-ap";

        final CapturingInterceptor interceptor = new CapturingInterceptor();

        try (
            S3Client client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .serviceConfiguration(
                    S3Configuration.builder()
                        // Ensure ARN region behavior is enabled; harmless for this test if already defaulted.
                        .useArnRegionEnabled(true)
                        .build()
                )
                .overrideConfiguration(ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build())
                .httpClient(failingHttpClient())
                // IMPORTANT: do NOT set endpointOverride(...) in these SDK tests.
                .build()
        ) {

            issueNoNetworkRequest(client, bucketArn);
        }

        final URI uri = interceptor.capturedUri();
        assertNotNull("SDK did not reach beforeTransmission; endpoint not captured", uri);
        assertNotNull(uri.getHost());
        assertTrue("Expected outposts endpoint but was: " + uri, uri.getHost().contains("s3-outposts"));
    }

    public void testSdkResolvesAccessPointEndpointFromBucketArn() {
        final String bucketArn = "arn:aws:s3:us-west-2:111122223333:accesspoint/my-ap";

        final CapturingInterceptor interceptor = new CapturingInterceptor();

        try (
            S3Client client = S3Client.builder()
                .region(Region.US_WEST_2)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .serviceConfiguration(S3Configuration.builder().useArnRegionEnabled(true).build())
                .overrideConfiguration(ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build())
                .httpClient(failingHttpClient())
                .build()
        ) {

            issueNoNetworkRequest(client, bucketArn);
        }

        final URI uri = interceptor.capturedUri();
        assertNotNull("SDK did not reach beforeTransmission; endpoint not captured", uri);
        assertNotNull(uri.getHost());
        assertTrue("Expected accesspoint endpoint but was: " + uri, uri.getHost().contains("s3-accesspoint"));
    }

    public void testSdkResolvesRegularBucketToRegularS3Endpoint() {
        final String bucket = "my-standard-bucket";

        final CapturingInterceptor interceptor = new CapturingInterceptor();

        try (
            S3Client client = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(AnonymousCredentialsProvider.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder().addExecutionInterceptor(interceptor).build())
                .httpClient(failingHttpClient())
                .build()
        ) {

            issueNoNetworkRequest(client, bucket);
        }

        final URI uri = interceptor.capturedUri();
        assertNotNull("SDK did not reach beforeTransmission; endpoint not captured", uri);
        assertNotNull(uri.getHost());

        final String host = uri.getHost();
        // Be tolerant across SDK versions/partitions: just assert it isn't ARN-specific routing.
        assertFalse("Unexpected accesspoint host for non-ARN bucket: " + uri, host.contains("s3-accesspoint"));
        assertFalse("Unexpected outposts host for non-ARN bucket: " + uri, host.contains("s3-outposts"));
        assertTrue("Expected some form of S3 host but was: " + uri, host.contains("s3"));
    }
}
