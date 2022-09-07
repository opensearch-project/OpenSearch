/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.get;

import org.opensearch.action.support.master.info.ClusterInfoRequest;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.is;

public class GetIndexRequestTests extends OpenSearchTestCase {
    public void testGetIndexRequestExtendsClusterInfoRequestOfDeprecatedClassPath() {
        GetIndexRequest getIndexRequest = new GetIndexRequest().indices("test");
        assertThat(getIndexRequest instanceof ClusterInfoRequest, is(true));
    }
}
