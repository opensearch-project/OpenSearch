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

package org.opensearch.rest;

import org.opensearch.common.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulate multiple handlers for the same path, allowing different handlers for different HTTP verbs.
 */
final class RestMethodHandlers implements MethodHandlers {

    private final String path;
    private final Map<RestRequest.Method, RestHandler> methodHandlers;

    RestMethodHandlers(String path, RestHandler handler, RestRequest.Method... methods) {
        this.path = path;
        this.methodHandlers = new HashMap<>(methods.length);
        for (RestRequest.Method method : methods) {
            methodHandlers.put(method, handler);
        }
    }

    /**
     * Add a handler for an additional array of methods. Note that {@code MethodHandlers}
     * does not allow replacing the handler for an already existing method.
     */
    public RestMethodHandlers addMethods(RestHandler handler, RestRequest.Method... methods) {
        for (RestRequest.Method method : methods) {
            RestHandler existing = methodHandlers.putIfAbsent(method, handler);
            if (existing != null) {
                throw new IllegalArgumentException("Cannot replace existing handler for [" + path + "] for method: " + method);
            }
        }
        return this;
    }

    /**
     * Returns the handler for the given method or {@code null} if none exists.
     */
    @Nullable
    public RestHandler getHandler(RestRequest.Method method) {
        return methodHandlers.get(method);
    }

    /**
     * Return a set of all valid HTTP methods for the particular path.
     */
    public Set<RestRequest.Method> getValidMethods() {
        return methodHandlers.keySet();
    }

    /**
     * Returns the relative HTTP path of the set of method handlers.
     */
    public String getPath() {
        return path;
    }
}
