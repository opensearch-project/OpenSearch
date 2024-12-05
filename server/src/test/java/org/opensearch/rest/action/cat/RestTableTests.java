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

package org.opensearch.rest.action.cat;

import org.opensearch.action.pagination.PageToken;
import org.opensearch.common.Table;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.rest.action.cat.RestTable.buildDisplayHeaders;
import static org.opensearch.rest.action.cat.RestTable.buildResponse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class RestTableTests extends OpenSearchTestCase {

    private static final String APPLICATION_JSON = MediaTypeRegistry.JSON.mediaType();
    private static final String APPLICATION_YAML = XContentType.YAML.mediaType();
    private static final String APPLICATION_SMILE = XContentType.SMILE.mediaType();
    private static final String APPLICATION_CBOR = XContentType.CBOR.mediaType();
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String ACCEPT = "Accept";
    private static final String TEXT_PLAIN = "text/plain; charset=UTF-8";
    private static final String TEXT_TABLE_BODY = "foo foo foo foo foo foo foo foo\n";
    private static final String PAGINATED_TEXT_TABLE_BODY = "foo foo foo foo foo foo foo foo\nnext_token foo\n";
    private static final String JSON_TABLE_BODY = "[{\"bulk.foo\":\"foo\",\"bulk.bar\":\"foo\",\"aliasedBulk\":\"foo\","
        + "\"aliasedSecondBulk\":\"foo\",\"unmatched\":\"foo\","
        + "\"invalidAliasesBulk\":\"foo\",\"timestamp\":\"foo\",\"epoch\":\"foo\"}]";
    private static final String PAGINATED_JSON_TABLE_BODY =
        "{\"next_token\":\"foo\",\"entities\":[{\"bulk.foo\":\"foo\",\"bulk.bar\":\"foo\",\"aliasedBulk\":\"foo\","
            + "\"aliasedSecondBulk\":\"foo\",\"unmatched\":\"foo\","
            + "\"invalidAliasesBulk\":\"foo\",\"timestamp\":\"foo\",\"epoch\":\"foo\"}]}";
    private static final String YAML_TABLE_BODY = "---\n"
        + "- bulk.foo: \"foo\"\n"
        + "  bulk.bar: \"foo\"\n"
        + "  aliasedBulk: \"foo\"\n"
        + "  aliasedSecondBulk: \"foo\"\n"
        + "  unmatched: \"foo\"\n"
        + "  invalidAliasesBulk: \"foo\"\n"
        + "  timestamp: \"foo\"\n"
        + "  epoch: \"foo\"\n";
    private static final String PAGINATED_YAML_TABLE_BODY = "---\n"
        + "next_token: \"foo\"\n"
        + "entities:\n"
        + "- bulk.foo: \"foo\"\n"
        + "  bulk.bar: \"foo\"\n"
        + "  aliasedBulk: \"foo\"\n"
        + "  aliasedSecondBulk: \"foo\"\n"
        + "  unmatched: \"foo\"\n"
        + "  invalidAliasesBulk: \"foo\"\n"
        + "  timestamp: \"foo\"\n"
        + "  epoch: \"foo\"\n";
    private Table table;
    private FakeRestRequest restRequest;

    @Before
    public void setup() {
        restRequest = new FakeRestRequest();
        table = new Table();
        addHeaders(table);
    }

    public void testThatDisplayHeadersSupportWildcards() throws Exception {
        restRequest.params().put("h", "bulk*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatDisplayHeadersAreNotAddedTwice() throws Exception {
        restRequest.params().put("h", "nonexistent,bulk*,bul*");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("unmatched")));
    }

    public void testThatWeUseTheAcceptHeaderJson() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_JSON)), APPLICATION_JSON, JSON_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderJsonForPaginatedTable() throws Exception {
        assertResponse(
            Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_JSON)),
            APPLICATION_JSON,
            PAGINATED_JSON_TABLE_BODY,
            getPaginatedTable()
        );
    }

    public void testThatWeUseTheAcceptHeaderYaml() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_YAML)), APPLICATION_YAML, YAML_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderYamlForPaginatedTable() throws Exception {
        assertResponse(
            Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_YAML)),
            APPLICATION_YAML,
            PAGINATED_YAML_TABLE_BODY,
            getPaginatedTable()
        );
    }

    public void testThatWeUseTheAcceptHeaderSmile() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_SMILE)), APPLICATION_SMILE);
    }

    public void testThatWeUseTheAcceptHeaderCbor() throws Exception {
        assertResponseContentType(Collections.singletonMap(ACCEPT, Collections.singletonList(APPLICATION_CBOR)), APPLICATION_CBOR);
    }

    public void testThatWeUseTheAcceptHeaderText() throws Exception {
        assertResponse(Collections.singletonMap(ACCEPT, Collections.singletonList(TEXT_PLAIN)), TEXT_PLAIN, TEXT_TABLE_BODY);
    }

    public void testThatWeUseTheAcceptHeaderTextForPaginatedTable() throws Exception {
        assertResponse(
            Collections.singletonMap(ACCEPT, Collections.singletonList(TEXT_PLAIN)),
            TEXT_PLAIN,
            PAGINATED_TEXT_TABLE_BODY,
            getPaginatedTable()
        );
    }

    public void testIgnoreContentType() throws Exception {
        assertResponse(Collections.singletonMap(CONTENT_TYPE, Collections.singletonList(APPLICATION_JSON)), TEXT_PLAIN, TEXT_TABLE_BODY);
    }

    public void testThatDisplayHeadersWithoutTimestamp() throws Exception {
        restRequest.params().put("h", "timestamp,epoch,bulk*");
        restRequest.params().put("ts", "false");
        List<RestTable.DisplayHeader> headers = buildDisplayHeaders(table, restRequest);

        List<String> headerNames = getHeaderNames(headers);
        assertThat(headerNames, contains("bulk.foo", "bulk.bar", "aliasedBulk", "aliasedSecondBulk"));
        assertThat(headerNames, not(hasItem("timestamp")));
        assertThat(headerNames, not(hasItem("epoch")));
    }

    public void testCompareRow() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();

        for (Integer i : Arrays.asList(1, 2, 1)) {
            table.startRow();
            table.addCell(i);
            table.endRow();
        }

        RestTable.TableIndexComparator comparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", false))
        );
        assertTrue(comparator.compare(0, 1) < 0);
        assertTrue(comparator.compare(0, 2) == 0);
        assertTrue(comparator.compare(1, 2) > 0);

        RestTable.TableIndexComparator reverseComparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", true))
        );

        assertTrue(reverseComparator.compare(0, 1) > 0);
        assertTrue(reverseComparator.compare(0, 2) == 0);
        assertTrue(reverseComparator.compare(1, 2) < 0);
    }

    public void testRowOutOfBounds() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();
        RestTable.TableIndexComparator comparator = new RestTable.TableIndexComparator(
            table,
            Collections.singletonList(new RestTable.ColumnOrderElement("compare", false))
        );
        Error e = expectThrows(AssertionError.class, () -> { comparator.compare(0, 1); });
        assertEquals("Invalid comparison of indices (0, 1): Table has 0 rows.", e.getMessage());
    }

    public void testUnknownHeader() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.endHeaders();
        restRequest.params().put("s", "notaheader");
        Exception e = expectThrows(UnsupportedOperationException.class, () -> RestTable.getRowOrder(table, restRequest));
        assertEquals("Unable to sort by unknown sort key `notaheader`", e.getMessage());
    }

    public void testAliasSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare", "alias:c;");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(3, 1, 2);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "c");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(1, 2, 0), rowOrder);
    }

    public void testReversedSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("reversed");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(0, 1, 2);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "reversed:desc");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(2, 1, 0), rowOrder);
    }

    public void testMultiSort() {
        Table table = new Table();
        table.startHeaders();
        table.addCell("compare");
        table.addCell("second.compare");
        table.endHeaders();
        List<Integer> comparisonList = Arrays.asList(3, 3, 2);
        List<Integer> secondComparisonList = Arrays.asList(2, 1, 3);
        for (int i = 0; i < comparisonList.size(); i++) {
            table.startRow();
            table.addCell(comparisonList.get(i));
            table.addCell(secondComparisonList.get(i));
            table.endRow();
        }
        restRequest.params().put("s", "compare,second.compare");
        List<Integer> rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(2, 1, 0), rowOrder);

        restRequest.params().put("s", "compare:desc,second.compare");
        rowOrder = RestTable.getRowOrder(table, restRequest);
        assertEquals(Arrays.asList(1, 0, 2), rowOrder);
    }

    private RestResponse assertResponseContentType(Map<String, List<String>> headers, String mediaType) throws Exception {
        return assertResponseContentType(headers, mediaType, table);
    }

    private RestResponse assertResponseContentType(Map<String, List<String>> headers, String mediaType, Table table) throws Exception {
        FakeRestRequest requestWithAcceptHeader = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(headers).build();
        table.startRow();
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.addCell("foo");
        table.endRow();
        RestResponse response = buildResponse(table, new AbstractRestChannel(requestWithAcceptHeader, true) {
            @Override
            public void sendResponse(RestResponse response) {}
        });

        assertThat(response.contentType(), equalTo(mediaType));
        return response;
    }

    private void assertResponse(Map<String, List<String>> headers, String mediaType, String body) throws Exception {
        assertResponse(headers, mediaType, body, table);
    }

    private void assertResponse(Map<String, List<String>> headers, String mediaType, String body, Table table) throws Exception {
        RestResponse response = assertResponseContentType(headers, mediaType, table);
        assertThat(response.content().utf8ToString(), equalTo(body));
    }

    private List<String> getHeaderNames(List<RestTable.DisplayHeader> headers) {
        List<String> headerNames = new ArrayList<>();
        for (RestTable.DisplayHeader header : headers) {
            headerNames.add(header.name);
        }

        return headerNames;
    }

    private Table getPaginatedTable() {
        PageToken pageToken = new PageToken("foo", "entities");
        Table paginatedTable = new Table(pageToken);
        addHeaders(paginatedTable);
        return paginatedTable;
    }

    private void addHeaders(Table table) {
        table.startHeaders();
        table.addCell("bulk.foo", "alias:f;desc:foo");
        table.addCell("bulk.bar", "alias:b;desc:bar");
        // should be matched as well due to the aliases
        table.addCell("aliasedBulk", "alias:bulkWhatever;desc:bar");
        table.addCell("aliasedSecondBulk", "alias:foobar,bulkolicious,bulkotastic;desc:bar");
        // no match
        table.addCell("unmatched", "alias:un.matched;desc:bar");
        // invalid alias
        table.addCell("invalidAliasesBulk", "alias:,,,;desc:bar");
        // timestamp
        table.addCell("timestamp", "alias:ts");
        table.addCell("epoch", "alias:t");
        table.endHeaders();
    }
}
