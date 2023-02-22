/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.io.Streams;

import java.io.IOException;

public class RefreshVersionInClusterStateIT extends AbstractRollingTestCase {

    /*
     * This test ensures that after the upgrade, all nodes report the current version
     */
    public void testRefresh() throws IOException {
        switch (CLUSTER_TYPE) {
            case OLD:
            case MIXED:
                break;
            case UPGRADED:
                Response response = client().performRequest(new Request("GET", "/_cat/nodes?h=id,version"));
                for (String nodeLine : Streams.readAllLines(response.getEntity().getContent())) {
                    String[] elements = nodeLine.split(" +");
                    assertEquals(Version.fromString(elements[1]), Version.CURRENT);
                }
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }
}
