/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import org.opensearch.core.action.ActionListener;

/**
 * A simple implementation of {@link ActionListener} that captures the response and failures used for testing purposes.
 *
 * @param <T> the result type
 */
public class TestCapturingListener<T> implements ActionListener<T> {
    private T result;
    private Exception failure;

    @Override
    public void onResponse(T result) {
        this.result = result;
    }

    @Override
    public void onFailure(Exception e) {
        this.failure = e;
    }

    public T getResult() {
        return result;
    }

    public Exception getFailure() {
        return failure;
    }
}
