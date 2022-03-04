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

package org.opensearch.common.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates logic for a one-way gate. Guarantees idempotency via the {@link AtomicBoolean} instance
 * and the return value of the {@link #close()} function.
 */
public class OneWayGate {

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Closes the gate and sets the internal boolean value in an idempotent
     * fashion. This is a one-way operation and cannot be reset.
     * @return true if the gate was closed in this invocation,
     * false if the gate was already closed
     */
    public boolean close() {
        return closed.compareAndSet(false, true);
    }

    /**
     * Indicates if the gate has been closed.
     * @return true if the gate is closed, false otherwise
     */
    public boolean isClosed() {
        return closed.get();
    }
}
