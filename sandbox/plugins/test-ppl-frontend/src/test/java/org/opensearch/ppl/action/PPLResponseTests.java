/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class PPLResponseTests extends OpenSearchTestCase {

    public void testToXContentEmptyResponse() throws IOException {
        PPLResponse response = new PPLResponse(List.of(), List.of());
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertEquals("{\"columns\":[],\"rows\":[]}", json);
    }

    public void testToXContentWithData() throws IOException {
        List<String> columns = List.of("name", "age");
        List<Object[]> rows = List.of(new Object[] { "Alice", 30 }, new Object[] { "Bob", 25 });
        PPLResponse response = new PPLResponse(columns, rows);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertEquals("{\"columns\":[\"name\",\"age\"],\"rows\":[[\"Alice\",30],[\"Bob\",25]]}", json);
    }

    public void testToXContentWithNullValues() throws IOException {
        List<String> columns = List.of("col1");
        List<Object[]> rows = new java.util.ArrayList<>();
        rows.add(new Object[] { null });
        PPLResponse response = new PPLResponse(columns, rows);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();
        assertEquals("{\"columns\":[\"col1\"],\"rows\":[[null]]}", json);
    }
}
