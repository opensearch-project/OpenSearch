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

package org.opensearch.transport;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;

/**
 * Options for transport requests
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TransportRequestOptions {

    private final TimeValue timeout;
    private final Type type;

    private TransportRequestOptions(TimeValue timeout, Type type) {
        this.timeout = timeout;
        this.type = type;
    }

    public TimeValue timeout() {
        return this.timeout;
    }

    public Type type() {
        return this.type;
    }

    public static final TransportRequestOptions EMPTY = new TransportRequestOptions.Builder().build();

    /**
     * Type of transport request
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Type {
        RECOVERY,
        BULK,
        REG,
        STATE,
        PING,
        STREAM
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for transport request options
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {
        private TimeValue timeout;
        private Type type = Type.REG;

        private Builder() {}

        public Builder withTimeout(long timeout) {
            return withTimeout(TimeValue.timeValueMillis(timeout));
        }

        public Builder withTimeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withType(Type type) {
            this.type = type;
            return this;
        }

        public TransportRequestOptions build() {
            return new TransportRequestOptions(timeout, type);
        }
    }
}
