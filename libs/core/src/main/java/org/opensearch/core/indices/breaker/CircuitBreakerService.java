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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.common.breaker.CircuitBreaker;

/**
 * Interface for Circuit Breaker services, which provide breakers to classes
 * that load field data.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class CircuitBreakerService extends AbstractLifecycleComponent {
    protected CircuitBreakerService() {}

    /**
     * @return the breaker that can be used to register estimates against
     */
    public abstract CircuitBreaker getBreaker(String name);

    /**
     * @return stats about all breakers
     */
    public abstract AllCircuitBreakerStats stats();

    /**
     * @return stats about a specific breaker
     */
    public abstract CircuitBreakerStats stats(String name);

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

}
