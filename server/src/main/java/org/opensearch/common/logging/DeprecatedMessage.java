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

package org.opensearch.common.logging;

import java.util.Map;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.core.common.Strings;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A logger message used by {@link DeprecationLogger}.
 * Carries x-opaque-id field if provided in the headers. Will populate the x-opaque-id field in JSON logs.
 *
 * @opensearch.internal
 */
public class DeprecatedMessage extends OpenSearchLogMessage {
    public static final String X_OPAQUE_ID_FIELD_NAME = "x-opaque-id";
    private static final Set<String> keys = ConcurrentHashMap.newKeySet();
    private final String keyWithXOpaqueId;

    public DeprecatedMessage(String key, String xOpaqueId, String messagePattern, Object... args) {
        super(fieldMap(key, xOpaqueId), messagePattern, args);
        this.keyWithXOpaqueId = new StringBuilder().append(key).append(xOpaqueId).toString();
    }

    /**
     * This method is to reset the key set which is used to log unique deprecation logs only.
     * The key set helps avoiding the deprecation messages being logged multiple times.
     * This method is a utility to reset this set for tests so they can run independent of each other.
     * Otherwise, a warning can be logged by some test and the upcoming test can be impacted by it.
     */
    public static void resetDeprecatedMessageForTests() {
        keys.clear();
    }

    private static Map<String, Object> fieldMap(String key, String xOpaqueId) {
        final MapBuilder<String, Object> builder = MapBuilder.newMapBuilder();
        if (Strings.isNullOrEmpty(key) == false) {
            builder.put("key", key);
        }
        if (Strings.isNullOrEmpty(xOpaqueId) == false) {
            builder.put(X_OPAQUE_ID_FIELD_NAME, xOpaqueId);
        }
        return builder.immutableMap();
    }

    public boolean isAlreadyLogged() {
        return !keys.add(keyWithXOpaqueId);
    }
}
