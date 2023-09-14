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

package org.opensearch.core.common.breaker;

/**
 * A {@link CircuitBreaker} that doesn't increment or adjust, and all operations are
 * basically noops.
 * It never trips, limit is always -1, always returns 0 for all metrics.
 * @opensearch.internal
 */
public class NoopCircuitBreaker implements CircuitBreaker {

    /** The limit of this breaker is always -1 */
    public static final int LIMIT = -1;
    /** Name of this breaker */
    private final String name;

    /**
     * Creates a new NoopCircuitBreaker (that never trip) with the given name
     * @param name the name of this breaker
     */
    public NoopCircuitBreaker(String name) {
        this.name = name;
    }

    /**
     * This is a noop, a noop breaker never trip
     * @param fieldName name of this noop breaker
     * @param bytesNeeded bytes needed
     */
    @Override
    public void circuitBreak(String fieldName, long bytesNeeded) {
        // noop
    }

    /**
     * This is a noop, always return 0 and never throw/trip
     * @param bytes number of bytes to add
     * @param label string label describing the bytes being added
     * @return always return 0
     * @throws CircuitBreakingException never thrown
     */
    @Override
    public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
        return 0;
    }

    /**
     * This is a noop, nothing is added, always return 0
     * @param bytes number of bytes to add (ignored)
     * @return always return 0
     */
    @Override
    public long addWithoutBreaking(long bytes) {
        return 0;
    }

    /**
     * This is a noop, always return 0
     * @return always return 0
     */
    @Override
    public long getUsed() {
        return 0;
    }

    /**
     * A noop breaker have a constant limit of -1
     * @return always return -1
     */
    @Override
    public long getLimit() {
        return LIMIT;
    }

    /**
     * A noop breaker have no overhead, always return 0
     * @return always return 0
     */
    @Override
    public double getOverhead() {
        return 0;
    }

    /**
     * A noop breaker never trip, always return 0
     * @return always return 0
     */
    @Override
    public long getTrippedCount() {
        return 0;
    }

    /**
     * return the name of this breaker
     * @return the name of this breaker
     */
    @Override
    public String getName() {
        return this.name;
    }

    /**
     * A noop breaker {@link Durability} is always {@link Durability#PERMANENT}
     * @return always return {@link Durability#PERMANENT }
     */
    @Override
    public Durability getDurability() {
        return Durability.PERMANENT;
    }

    /**
     * Limit and overhead are constant for a noop breaker.
     * this is a noop.
     * @param limit the desired limit (ignored)
     * @param overhead the desired overhead (ignored)
     */
    @Override
    public void setLimitAndOverhead(long limit, double overhead) {
        // noop
    }
}
