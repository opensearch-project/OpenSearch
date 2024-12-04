/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This enum contains the type of entities a resource can be shared with.
 *
 * @opensearch.experimental
 */
public enum EntityType {

    USERS("users"),
    ROLES("roles"),
    BACKEND_ROLES("backend_roles");

    private static final Map<String, EntityType> VALUE_MAP = Arrays.stream(values())
        .collect(Collectors.toMap(EntityType::toString, Function.identity()));

    private final String value;

    EntityType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static EntityType fromValue(String value) {
        EntityType type = VALUE_MAP.get(value);
        if (type == null) {
            throw new IllegalArgumentException("No enum constant with value: " + value);
        }
        return type;
    }
}
