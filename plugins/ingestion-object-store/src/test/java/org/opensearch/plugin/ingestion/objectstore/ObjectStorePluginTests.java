/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link ObjectStorePlugin} registration of its ingestion source type and factory.
 */
public class ObjectStorePluginTests extends OpenSearchTestCase {

    private final ObjectStorePlugin plugin = new ObjectStorePlugin();

    public void testGetType() {
        assertThat(plugin.getType(), equalTo("OBJECT_STORE"));
        assertThat(ObjectStorePlugin.TYPE, equalTo("OBJECT_STORE"));
    }

    @SuppressWarnings("rawtypes")
    public void testGetIngestionConsumerFactories() {
        Map<String, IngestionConsumerFactory> factories = plugin.getIngestionConsumerFactories();
        assertThat(factories, hasKey(ObjectStorePlugin.TYPE));
        assertThat(factories.get(ObjectStorePlugin.TYPE), is(instanceOf(ObjectStoreConsumerFactory.class)));
    }
}
