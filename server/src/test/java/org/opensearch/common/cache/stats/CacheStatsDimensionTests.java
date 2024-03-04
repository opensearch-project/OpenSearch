/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

public class CacheStatsDimensionTests extends OpenSearchTestCase {
    public void testSerialization() throws Exception {
        String name = "dimension_name";
        String value = "dimension_value";
        CacheStatsDimension dim = new CacheStatsDimension(name, value);

        BytesStreamOutput os = new BytesStreamOutput();
        dim.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        CacheStatsDimension deserialized = new CacheStatsDimension(is);

        assertEquals(dim.dimensionName, deserialized.dimensionName);
        assertEquals(dim.dimensionValue, deserialized.dimensionValue);
        assertEquals(dim, deserialized);
    }

    public void testEquality() throws Exception {
        String name = "dimension_name";
        String value = "dimension_value";
        CacheStatsDimension dim = new CacheStatsDimension(name, value);
        assertEquals(dim, new CacheStatsDimension(name, value));
        assertNotEquals(dim, new CacheStatsDimension("a", "b"));
        assertNotEquals(dim, null);
        assertNotEquals(dim, new CacheStatsDimension(null, null));
    }
}
