/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import com.google.api.gax.retrying.ResultRetryAlgorithm;
import com.google.api.gax.retrying.TimedAttemptSettings;
import com.google.cloud.BaseService;
import com.google.cloud.storage.StorageRetryStrategy;
import org.opensearch.ExceptionsHelper;

import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CancellationException;

import static java.util.Objects.nonNull;

public class GoogleShouldRetryStorageStrategy implements StorageRetryStrategy {

    private final DelagateResultRetryAlgorithm<?> idempotentHandler = new DelagateResultRetryAlgorithm<>(BaseService.EXCEPTION_HANDLER);

    private final DelagateResultRetryAlgorithm<?> nonIdempotentHandler = new DelagateResultRetryAlgorithm<>(BaseService.EXCEPTION_HANDLER);

    private static final class DelagateResultRetryAlgorithm<T> implements ResultRetryAlgorithm<T> {

        private final ResultRetryAlgorithm<T> resultRetryAlgorithm;

        private DelagateResultRetryAlgorithm(ResultRetryAlgorithm<T> resultRetryAlgorithm) {
            this.resultRetryAlgorithm = resultRetryAlgorithm;
        }

        @Override
        public TimedAttemptSettings createNextAttempt(Throwable prevThrowable, T prevResponse, TimedAttemptSettings prevSettings) {
            return resultRetryAlgorithm.createNextAttempt(prevThrowable, prevResponse, prevSettings);
        }

        @Override
        public boolean shouldRetry(Throwable prevThrowable, T prevResponse) throws CancellationException {
            if (nonNull(ExceptionsHelper.unwrap(prevThrowable, UnknownHostException.class))) {
                return true;
            }
            if (nonNull(ExceptionsHelper.unwrap(prevThrowable, SocketException.class))) {
                return true;
            }
            return resultRetryAlgorithm.shouldRetry(prevThrowable, prevResponse);
        }
    };

    @Override
    public ResultRetryAlgorithm<?> getIdempotentHandler() {
        return idempotentHandler;
    }

    @Override
    public ResultRetryAlgorithm<?> getNonidempotentHandler() {
        return nonIdempotentHandler;
    }
}
