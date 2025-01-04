/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import java.util.HashMap;
import java.util.Map;

/**
 * This class determines a collection of recipient types a resource can be shared with.
 *
 * @opensearch.experimental
 */
public class RecipientTypeRegistry {
    private static final Map<String, RecipientType> REGISTRY = new HashMap<>();

    public static void registerRecipientType(String key, RecipientType recipientType) {
        REGISTRY.put(key, recipientType);
    }

    public static RecipientType fromValue(String value) {
        RecipientType type = REGISTRY.get(value);
        if (type == null) {
            throw new IllegalArgumentException("Unknown RecipientType: " + value + ". Must be 1 of these: " + REGISTRY.values());
        }
        return type;
    }
}
