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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support;

import org.opensearch.OpenSearchException;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.BaseFuture;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.core.action.ActionListener;

import java.util.concurrent.TimeUnit;

/**
 * Future adapter action
 *
 * @opensearch.internal
 */
public abstract class AdapterActionFuture<T, L> extends BaseFuture<T> implements ActionFuture<T>, ActionListener<L> {

    @Override
    public T actionGet() {
        try {
            return FutureUtils.get(this);
        } catch (OpenSearchException e) {
            throw unwrapEsException(e);
        }
    }

    @Override
    public T actionGet(String timeout) {
        return actionGet(TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".actionGet.timeout"));
    }

    @Override
    public T actionGet(long timeoutMillis) {
        return actionGet(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(TimeValue timeout) {
        return actionGet(timeout.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public T actionGet(long timeout, TimeUnit unit) {
        try {
            return FutureUtils.get(this, timeout, unit);
        } catch (OpenSearchException e) {
            throw unwrapEsException(e);
        }
    }

    @Override
    public void onResponse(L result) {
        set(convert(result));
    }

    @Override
    public void onFailure(Exception e) {
        setException(e);
    }

    protected abstract T convert(L listenerResponse);

    private static RuntimeException unwrapEsException(OpenSearchException esEx) {
        Throwable root = esEx.unwrapCause();
        if (root instanceof RuntimeException) {
            return (RuntimeException) root;
        }
        return new UncategorizedExecutionException("Failed execution", root);
    }
}
