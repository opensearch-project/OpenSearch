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

package org.opensearch.discovery.gce;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.Sleeper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.secure_sm.AccessController;

import java.io.IOException;
import java.util.Objects;

public class RetryHttpInitializerWrapper implements HttpRequestInitializer {
    private static final Logger logger = LogManager.getLogger(RetryHttpInitializerWrapper.class);

    // Intercepts the request for filling in the "Authorization"
    // header field, as well as recovering from certain unsuccessful
    // error codes wherein the Credential must refresh its token for a
    // retry.
    private final Credential wrappedCredential;

    // A sleeper; you can replace it with a mock in your test.
    private final Sleeper sleeper;

    private TimeValue maxWait;

    public RetryHttpInitializerWrapper(Credential wrappedCredential) {
        this(wrappedCredential, Sleeper.DEFAULT, TimeValue.timeValueMillis(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS));
    }

    public RetryHttpInitializerWrapper(Credential wrappedCredential, TimeValue maxWait) {
        this(wrappedCredential, Sleeper.DEFAULT, maxWait);
    }

    // Use only for testing.
    RetryHttpInitializerWrapper(Credential wrappedCredential, Sleeper sleeper, TimeValue maxWait) {
        this.wrappedCredential = Objects.requireNonNull(wrappedCredential);
        this.sleeper = sleeper;
        this.maxWait = maxWait;
    }

    // Use only for testing
    static MockGoogleCredential.Builder newMockCredentialBuilder() {
        // TODO: figure out why GCE is so bad like this
        return AccessController.doPrivileged(MockGoogleCredential.Builder::new);
    }

    @Override
    public void initialize(HttpRequest httpRequest) {
        final HttpUnsuccessfulResponseHandler backoffHandler = new HttpBackOffUnsuccessfulResponseHandler(
            new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(((int) maxWait.getMillis())).build()
        ).setSleeper(sleeper);

        httpRequest.setInterceptor(wrappedCredential);
        httpRequest.setUnsuccessfulResponseHandler(new HttpUnsuccessfulResponseHandler() {
            int retry = 0;

            @Override
            public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry) throws IOException {
                if (wrappedCredential.handleResponse(request, response, supportsRetry)) {
                    // If credential decides it can handle it,
                    // the return code or message indicated
                    // something specific to authentication,
                    // and no backoff is desired.
                    return true;
                } else if (backoffHandler.handleResponse(request, response, supportsRetry)) {
                    // Otherwise, we defer to the judgement of
                    // our internal backoff handler.
                    logger.debug("Retrying [{}] times : [{}]", retry, request.getUrl());
                    return true;
                } else {
                    return false;
                }
            }
        });
        httpRequest.setIOExceptionHandler(
            new HttpBackOffIOExceptionHandler(new ExponentialBackOff.Builder().setMaxElapsedTimeMillis(((int) maxWait.getMillis())).build())
                .setSleeper(sleeper)
        );
    }
}
