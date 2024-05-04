/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi;

import org.opensearch.client.Client;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.util.List;

import static org.mockito.Mockito.mock;

public class UbiModulePluginTests extends OpenSearchTestCase {

    private UbiModulePlugin ubiModulePlugin;

    private final Client client = mock(Client.class);

    @Before
    public void setup() {
        ubiModulePlugin = new UbiModulePlugin();
    }

    public void testCreateComponent() {
        List<Object> components = (List<Object>) ubiModulePlugin.createComponents(
            client,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        Assert.assertEquals(0, components.size());
    }

    public void testGetSearchExts() {

        final List<SearchPlugin.SearchExtSpec<?>> searchExts = ubiModulePlugin.getSearchExts();
        Assert.assertEquals(1, searchExts.size());

    }

}
