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

package org.opensearch.client.indices;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.client.AbstractRequestTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

public class ResizeRequestTests extends AbstractRequestTestCase<ResizeRequest, org.opensearch.action.admin.indices.shrink.ResizeRequest> {

    @Override
    protected ResizeRequest createClientTestInstance() {
        return new ResizeRequest("target", "source").setAliases(Arrays.asList(new Alias("target1"), new Alias("target2")))
            .setSettings(Settings.builder().put("index.foo", "bar").build());
    }

    @Override
    protected org.opensearch.action.admin.indices.shrink.ResizeRequest doParseToServerInstance(XContentParser parser) throws IOException {
        org.opensearch.action.admin.indices.shrink.ResizeRequest req = new org.opensearch.action.admin.indices.shrink.ResizeRequest(
            "target",
            "source"
        );
        req.fromXContent(parser);
        return req;
    }

    @Override
    protected void assertInstances(
        org.opensearch.action.admin.indices.shrink.ResizeRequest serverInstance,
        ResizeRequest clientTestInstance
    ) {
        assertEquals(serverInstance.getSourceIndex(), clientTestInstance.getSourceIndex());
        assertEquals(serverInstance.getTargetIndexRequest().index(), clientTestInstance.getTargetIndex());
        assertEquals(serverInstance.getTargetIndexRequest().settings(), clientTestInstance.getSettings());
        assertEquals(serverInstance.getTargetIndexRequest().aliases(), clientTestInstance.getAliases());
    }
}
