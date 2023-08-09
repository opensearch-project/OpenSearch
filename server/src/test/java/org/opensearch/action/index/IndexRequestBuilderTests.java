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

package org.opensearch.action.index;

import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class IndexRequestBuilderTests extends OpenSearchTestCase {

    private static final String EXPECTED_SOURCE = "{\"SomeKey\":\"SomeValue\"}";
    private NoOpClient testClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.testClient = new NoOpClient(getTestName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.testClient.close();
        super.tearDown();
    }

    /**
     * test setting the source for the request with different available setters
     */
    public void testSetSource() throws Exception {
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(this.testClient, IndexAction.INSTANCE);
        Map<String, String> source = new HashMap<>();
        source.put("SomeKey", "SomeValue");
        indexRequestBuilder.setSource(source);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder.setSource(source, MediaTypeRegistry.JSON);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder.setSource("SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        // force the Object... setter
        indexRequestBuilder.setSource((Object) "SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        XContentBuilder doc = XContentFactory.jsonBuilder(docOut).startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(docOut.toByteArray(), MediaTypeRegistry.JSON);
        assertEquals(
            EXPECTED_SOURCE,
            XContentHelper.convertToJson(indexRequestBuilder.request().source(), true, indexRequestBuilder.request().getContentType())
        );

        doc = XContentFactory.jsonBuilder().startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(doc);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));
    }
}
