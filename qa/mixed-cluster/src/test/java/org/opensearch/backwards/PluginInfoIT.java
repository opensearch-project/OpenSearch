/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.backwards;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.util.Map;

public class PluginInfoIT extends OpenSearchRestTestCase {
    public void testPluginInfoSerialization() throws Exception {
        // Ensure all nodes are able to come up, validate with GET _nodes.
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        assertEquals(4, nodeMap.keySet().size());
    }
}
