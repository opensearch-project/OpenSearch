/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.main;

import java.io.IOException;

import org.opensearch.Build;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.OpenSearchTestCase;

public class EvilSystemPropertyTests extends OpenSearchTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testToXContent_responseVersionOverride() throws IOException {
        System.setProperty("opensearch.http.override_main_response_version", "true");
        try {
            MainResponse response = new MainResponse("nodeName", Version.CURRENT,
                new ClusterName("clusterName"), randomAlphaOfLengthBetween(10, 20), Build.CURRENT);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertTrue(Strings.toString(builder).contains("\"number\":\"" + LegacyESVersion.V_7_10_2.toString() + "\","));
        } finally {
            System.clearProperty("opensearch.http.override_main_response_version");
        }
    }
}
