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

package org.opensearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.opensearch.core.ParseField;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;

/**
 * This test works against a {@link RestHighLevelClient} subclass that simulates how custom response sections returned by
 * OpenSearch plugins can be parsed using the high level client.
 */
public class RestHighLevelClientExtTests extends OpenSearchTestCase {

    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() {
        RestClient restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClientExt(restClient);
    }

    public void testParseEntityCustomResponseSection() throws IOException {
        {
            HttpEntity jsonEntity = new NStringEntity("{\"custom1\":{ \"field\":\"value\"}}", ContentType.APPLICATION_JSON);
            BaseCustomResponseSection customSection = restHighLevelClient.parseEntity(jsonEntity, BaseCustomResponseSection::fromXContent);
            assertThat(customSection, instanceOf(CustomResponseSection1.class));
            CustomResponseSection1 customResponseSection1 = (CustomResponseSection1) customSection;
            assertEquals("value", customResponseSection1.value);
        }
        {
            HttpEntity jsonEntity = new NStringEntity("{\"custom2\":{ \"array\": [\"item1\", \"item2\"]}}", ContentType.APPLICATION_JSON);
            BaseCustomResponseSection customSection = restHighLevelClient.parseEntity(jsonEntity, BaseCustomResponseSection::fromXContent);
            assertThat(customSection, instanceOf(CustomResponseSection2.class));
            CustomResponseSection2 customResponseSection2 = (CustomResponseSection2) customSection;
            assertArrayEquals(new String[] { "item1", "item2" }, customResponseSection2.values);
        }
    }

    private static class RestHighLevelClientExt extends RestHighLevelClient {

        private RestHighLevelClientExt(RestClient restClient) {
            super(restClient, RestClient::close, getNamedXContentsExt());
        }

        private static List<NamedXContentRegistry.Entry> getNamedXContentsExt() {
            List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
            entries.add(
                new NamedXContentRegistry.Entry(
                    BaseCustomResponseSection.class,
                    new ParseField("custom1"),
                    CustomResponseSection1::fromXContent
                )
            );
            entries.add(
                new NamedXContentRegistry.Entry(
                    BaseCustomResponseSection.class,
                    new ParseField("custom2"),
                    CustomResponseSection2::fromXContent
                )
            );
            return entries;
        }
    }

    private abstract static class BaseCustomResponseSection {

        static BaseCustomResponseSection fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            BaseCustomResponseSection custom = parser.namedObject(BaseCustomResponseSection.class, parser.currentName(), null);
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return custom;
        }
    }

    private static class CustomResponseSection1 extends BaseCustomResponseSection {

        private final String value;

        private CustomResponseSection1(String value) {
            this.value = value;
        }

        static CustomResponseSection1 fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("field", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            CustomResponseSection1 responseSection1 = new CustomResponseSection1(parser.text());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return responseSection1;
        }
    }

    private static class CustomResponseSection2 extends BaseCustomResponseSection {

        private final String[] values;

        private CustomResponseSection2(String[] values) {
            this.values = values;
        }

        static CustomResponseSection2 fromXContent(XContentParser parser) throws IOException {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("array", parser.currentName());
            assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            List<String> values = new ArrayList<>();
            while (parser.nextToken().isValue()) {
                values.add(parser.text());
            }
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            CustomResponseSection2 responseSection2 = new CustomResponseSection2(values.toArray(new String[0]));
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            return responseSection2;
        }
    }
}
