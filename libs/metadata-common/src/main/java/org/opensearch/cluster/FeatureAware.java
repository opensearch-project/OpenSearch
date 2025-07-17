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

package org.opensearch.cluster;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VersionedNamedWriteable;

import java.util.Optional;

/**
 * An interface that implementors use when a class requires a client to maybe have a feature.
 *
 * @opensearch.internal
 */
public interface FeatureAware {

    /**
     * An optional feature that is required for the client to have.
     *
     * @return an empty optional if no feature is required otherwise a string representing the required feature
     */
    default Optional<String> getRequiredFeature() {
        return Optional.empty();
    }

    /**
     * Tests whether the custom should be serialized. The criterion is that
     * the output stream must be at least the minimum supported version of the custom.
     * <p>
     * That is, we only serialize customs to clients than can understand the custom based on the version of the client.
     *
     * @param out    the output stream
     * @param custom the custom to serialize
     * @param <T>    the type of the custom
     * @return true if the custom should be serialized and false otherwise
     */
    static <T extends VersionedNamedWriteable & FeatureAware> boolean shouldSerialize(final StreamOutput out, final T custom) {
        return out.getVersion().onOrAfter(custom.getMinimalSupportedVersion());
    }
}
