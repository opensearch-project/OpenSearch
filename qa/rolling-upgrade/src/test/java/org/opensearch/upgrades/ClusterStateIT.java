/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.util.Map;

public class ClusterStateIT extends AbstractRollingTestCase{
    public void testTemplateMetadataUpgrades() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            String templateName = "my_template";
            Request putIndexTemplate = new Request("PUT", "_template/" + templateName);
            putIndexTemplate.setJsonEntity("{\"index_patterns\": [\"pattern-1\", \"log-*\"]}");
            client().performRequest(putIndexTemplate);
            verifyTemplateMetadataInClusterState();
        } else {
            verifyTemplateMetadataInClusterState();
        }
    }

    @SuppressWarnings("unchecked")
    private static void verifyTemplateMetadataInClusterState() throws Exception {
        Request request = new Request("GET", "_cluster/state/metadata");
        Response response = client().performRequest(request);
        assertOK(response);
        Map<String, Object> metadata = (Map<String, Object>) entityAsMap(response).get("metadata");
        assertNotNull(metadata.get("templates"));
    }
}
