/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.http.async.SdkAsyncHttpClient;

/**
 * An interface for providing the specific implementation of HTTP client used for s3 client.
 * <p>
 * Implementations are responsible implement the asyncHttpClient method and provide the appropriate Http client
 * to be used by s3 clients.
 * <p>
 * Current supported Http clients are "AwsCrtAsyncHttpClient" and "NettyNioAsyncHttpClient"
 */
public interface AsyncHttpClient {

    /**
     * Method to build the appropriate HttpClient implementations
     * @return SdkAsyncHttpClient
     */
    SdkAsyncHttpClient asyncHttpClient();
}
