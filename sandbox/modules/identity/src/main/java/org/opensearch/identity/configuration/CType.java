/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import org.opensearch.identity.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public enum CType {

    INTERNALUSERS(toMap(1, User.class));

    private Map<Integer, Class<?>> implementations;

    private CType(Map<Integer, Class<?>> implementations) {
        this.implementations = implementations;
    }

    public Map<Integer, Class<?>> getImplementationClass() {
        return Collections.unmodifiableMap(implementations);
    }

    public static CType fromString(String value) {
        return CType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    public String toLCString() {
        return this.toString().toLowerCase(Locale.ROOT);
    }

    private static Map<Integer, Class<?>> toMap(Object... objects) {
        final Map<Integer, Class<?>> map = new HashMap<Integer, Class<?>>();
        for (int i = 0; i < objects.length; i = i + 2) {
            map.put((Integer) objects[i], (Class<?>) objects[i + 1]);
        }
        return Collections.unmodifiableMap(map);
    }
}
