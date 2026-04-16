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

public class SeqNoFieldMapperTests extends OpenSearchTestCase {

    public void testDefaultCapabilities() {
        SeqNoFieldMapper mapper = new SeqNoFieldMapper();
        Set<FieldTypeCapabilities.Capability> caps = mapper.fieldType().defaultCapabilities();
        assertEquals(2, caps.size());
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.POINT_RANGE));
        assertTrue(caps.contains(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
    }
}
