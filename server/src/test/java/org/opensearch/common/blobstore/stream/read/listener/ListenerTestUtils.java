/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.core.action.ActionListener;

/**
 * Utility class containing common functionality for read listener based tests
 */
public class ListenerTestUtils {

    /**
     * CountingCompletionListener acts as a verification instance for wrapping listener based calls.
     * Keeps track of the last response, failure and count of response and failure invocations.
     */
    static class CountingCompletionListener<T> implements ActionListener<T> {
        private int responseCount;
        private int failureCount;
        private T response;
        private Exception exception;

        @Override
        public void onResponse(T response) {
            this.response = response;
            responseCount++;
        }

        @Override
        public void onFailure(Exception e) {
            exception = e;
            failureCount++;
        }

        public int getResponseCount() {
            return responseCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public T getResponse() {
            return response;
        }

        public Exception getException() {
            return exception;
        }
    }
}
