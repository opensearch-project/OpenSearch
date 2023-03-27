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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;

public class RestResizeHandlerTests extends OpenSearchTestCase {

    private Set<String> assertedWarnings = new HashSet<>();

    public void testShrinkCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestShrinkIndexAction handler = new RestResizeHandler.RestShrinkIndexAction();
        for (final String copySettings : new String[] { null, "", "true", "false" }) {
            runTestResizeCopySettingsDeprecated(handler, "shrink", copySettings);
        }
    }

    public void testSplitCopySettingsDeprecated() throws IOException {
        final RestResizeHandler.RestSplitIndexAction handler = new RestResizeHandler.RestSplitIndexAction();
        for (final String copySettings : new String[] { null, "", "true", "false" }) {
            runTestResizeCopySettingsDeprecated(handler, "split", copySettings);
        }
    }

    private void runTestResizeCopySettingsDeprecated(
        final RestResizeHandler handler,
        final String resizeOperation,
        final String copySettings
    ) throws IOException {
        final FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Collections.singletonMap("copy_settings", copySettings)
        ).withPath(String.format(Locale.ROOT, "source/_%s/target", resizeOperation));
        if (copySettings != null) {
            builder.withParams(Collections.singletonMap("copy_settings", copySettings));
        }
        final FakeRestRequest request = builder.build();
        if ("false".equals(copySettings)) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> handler.prepareRequest(request, mock(NodeClient.class))
            );
            assertThat(e, hasToString(containsString("parameter [copy_settings] can not be explicitly set to [false]")));
        } else {
            String expectedWarning = "parameter [copy_settings] is deprecated and will be removed in 3.0.0";
            handler.prepareRequest(request, mock(NodeClient.class));
            if (("".equals(copySettings) || "true".equals(copySettings)) && !assertedWarnings.contains(expectedWarning)) {
                assertWarnings(expectedWarning);
                assertedWarnings.add(expectedWarning);
            }
        }
    }

}
