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
 * Interface for the AsyncHttpClient to be used by S3 for file uploads.
 */
public interface AsyncHttpClient {

    /**
     * method to build the appropriate HttpClient implementations
     * @return SdkAsyncHttpClient
     */
    SdkAsyncHttpClient asyncHttpClient();
}
