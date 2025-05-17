/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.mappingtransformer;

import org.opensearch.index.mapper.MappingTransformer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class ExampleMappingTransformerPluginTests extends OpenSearchTestCase {
    private final ExampleMappingTransformerPlugin plugin = new ExampleMappingTransformerPlugin();

    public void testGetMappingTransformers() {
        List<MappingTransformer> mappingTransformerList = plugin.getMappingTransformers();

        assertTrue("Should return an example mapping transformer.", mappingTransformerList.getFirst() instanceof ExampleMappingTransformer);
    }
}
