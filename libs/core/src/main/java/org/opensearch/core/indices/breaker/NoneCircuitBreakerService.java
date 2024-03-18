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

package org.opensearch.core.indices.breaker;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;

/**
 * Class that returns a breaker that use the NoopCircuitBreaker and never breaks
 *
 * @see org.opensearch.core.common.breaker.NoopCircuitBreaker
 * @opensearch.internal
 */
public class NoneCircuitBreakerService extends CircuitBreakerService {

    private final CircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.FIELDDATA);

    public NoneCircuitBreakerService() {
        super();
    }

    /**
     * Returns a breaker that use the NoopCircuitBreaker and never breaks
     *
     * @param name name of the breaker (ignored)
     * @return a NoopCircuitBreaker
     */
    @Override
    public CircuitBreaker getBreaker(String name) {
        return breaker;
    }

    @Override
    public AllCircuitBreakerStats stats() {
        return new AllCircuitBreakerStats(new CircuitBreakerStats[] { stats(CircuitBreaker.FIELDDATA) });
    }

    /**
     * Always returns the same stats, a NoopCircuitBreaker never breaks and all operations are noops.
     *
     * @param name name of the breaker (ignored)
     * @return always "fielddata", limit: -1, estimated: -1, overhead: 0, trippedCount: 0
     */
    @Override
    public CircuitBreakerStats stats(String name) {
        return new CircuitBreakerStats(CircuitBreaker.FIELDDATA, -1, -1, 0, 0);
    }

}
