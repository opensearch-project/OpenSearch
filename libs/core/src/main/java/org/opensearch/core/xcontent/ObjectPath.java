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

package org.opensearch.core.xcontent;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Helper class to navigate nested objects using dot notation
 */
public final class ObjectPath {

    private static final String[] EMPTY_ARRAY = new String[0];

    private ObjectPath() {}

    /**
     * Return the value within a given object at the specified path, or
     * {@code null} if the path does not exist
     */
    @SuppressWarnings("unchecked")
    public static <T> T eval(String path, Object object) {
        return (T) evalContext(path, object);
    }

    private static Object evalContext(String path, Object ctx) {
        final String[] parts;
        if (path == null || path.isEmpty()) parts = EMPTY_ARRAY;
        else parts = path.split("\\.");
        for (String part : parts) {
            if (ctx == null) {
                return null;
            }
            if (ctx instanceof Map<?, ?> map) {
                ctx = map.get(part);
            } else if (ctx instanceof List<?> list) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = list.get(index);
                } catch (NumberFormatException nfe) {
                    return null;
                }
            } else if (ctx.getClass().isArray()) {
                try {
                    int index = Integer.parseInt(part);
                    ctx = Array.get(ctx, index);
                } catch (NumberFormatException nfe) {
                    return null;
                }
            } else {
                return null;
            }
        }
        return ctx;
    }
}
