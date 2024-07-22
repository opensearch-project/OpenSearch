/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.resourcetypes;

import org.opensearch.test.OpenSearchTestCase;

public class ResourceTypeTests extends OpenSearchTestCase {

    public void testFromNameMemory() {
        ResourceType result = ResourceType.fromName("memory");
        assertTrue(result instanceof Memory);
    }

    public void testFromNameCPU() {
        ResourceType result = ResourceType.fromName("CPU");
        assertTrue(result instanceof CPU);
    }

    public void testFromNameInvalid() {
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("invalid"); });
    }
}
