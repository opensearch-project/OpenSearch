/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterUpdateSettingsRequestTests extends OpenSearchTestCase {

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final ClusterUpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;
        BytesReference originalBytes = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        if (addRandomFields) {
            String unsupportedField = "unsupported_field";
            BytesReference mutated = BytesReference.bytes(
                XContentTestUtils.insertIntoXContent(
                    xContentType.xContent(),
                    originalBytes,
                    Collections.singletonList(""),
                    () -> unsupportedField,
                    () -> randomAlphaOfLengthBetween(3, 10)
                )
            );
            XContentParseException iae = expectThrows(
                XContentParseException.class,
                () -> ClusterUpdateSettingsRequest.fromXContent(createParser(xContentType.xContent(), mutated))
            );
            assertThat(iae.getMessage(), containsString("[cluster_update_settings_request] unknown field [" + unsupportedField + "]"));
        } else {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                ClusterUpdateSettingsRequest parsedRequest = ClusterUpdateSettingsRequest.fromXContent(parser);

                assertNull(parser.nextToken());
                assertThat(parsedRequest.transientSettings(), equalTo(request.transientSettings()));
                assertThat(parsedRequest.persistentSettings(), equalTo(request.persistentSettings()));
            }
        }
    }

    private static ClusterUpdateSettingsRequest createTestItem() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        request.transientSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        return request;
    }
}
