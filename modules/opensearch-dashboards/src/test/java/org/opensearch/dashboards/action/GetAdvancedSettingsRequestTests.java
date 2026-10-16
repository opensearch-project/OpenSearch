/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GetAdvancedSettingsRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        GetAdvancedSettingsRequest request = new GetAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0");

        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        GetAdvancedSettingsRequest deserialized = new GetAdvancedSettingsRequest(input);

        assertEquals(".opensearch_dashboards", deserialized.getIndex());
        assertEquals("config:3.7.0", deserialized.getDocumentId());
        assertArrayEquals(new String[] { ".opensearch_dashboards" }, deserialized.indices());
    }
}
