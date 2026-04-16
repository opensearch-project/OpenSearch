/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class VersionFieldMapperTests extends OpenSearchTestCase {

    public void testDefaultCapabilities() {
        Set<FieldTypeCapabilities.Capability> caps = VersionFieldMapper.VersionFieldType.INSTANCE.defaultCapabilities();
        assertEquals(1, caps.size());
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
    }
}
