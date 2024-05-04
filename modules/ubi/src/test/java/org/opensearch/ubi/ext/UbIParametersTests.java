/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi.ext;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentGenerator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UbIParametersTests extends OpenSearchTestCase {

    static class DummyStreamOutput extends StreamOutput {

        List<String> list = new ArrayList<>();
        List<Integer> intValues = new ArrayList<>();

        @Override
        public void writeString(String str) {
            list.add(str);
        }

        public List<String> getList() {
            return list;
        }

        @Override
        public void writeInt(int i) {
            intValues.add(i);
        }

        public List<Integer> getIntValues() {
            return this.intValues;
        }

        @Override
        public void writeByte(byte b) throws IOException {

        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {

        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void reset() throws IOException {

        }
    }

    public void testUbiParameters() {
        final UbiParameters params = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        UbiParametersExtBuilder extBuilder = new UbiParametersExtBuilder();
        extBuilder.setParams(params);
        SearchSourceBuilder srcBulder = SearchSourceBuilder.searchSource().ext(List.of(extBuilder));
        SearchRequest request = new SearchRequest("my_index").source(srcBulder);
        UbiParameters actual = UbiParameters.getUbiParameters(request);
        assertEquals(params, actual);
    }

    public void testWriteTo() throws IOException {
        final UbiParameters params = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        StreamOutput output = new DummyStreamOutput();
        params.writeTo(output);
        List<String> actual = ((DummyStreamOutput) output).getList();
        assertEquals("query_id", actual.get(0));
        assertEquals("user_query", actual.get(1));
        assertEquals("client_id", actual.get(2));
        assertEquals("object_id", actual.get(3));
    }

    public void testToXContent() throws IOException {
        final UbiParameters params = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        XContent xc = mock(XContent.class);
        OutputStream os = mock(OutputStream.class);
        XContentGenerator generator = mock(XContentGenerator.class);
        when(xc.createGenerator(any(), any(), any())).thenReturn(generator);
        XContentBuilder builder = new XContentBuilder(xc, os);
        assertNotNull(params.toXContent(builder, null));
    }

    public void testToXContentAllOptionalParameters() throws IOException {
        final UbiParameters params = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        XContent xc = mock(XContent.class);
        OutputStream os = mock(OutputStream.class);
        XContentGenerator generator = mock(XContentGenerator.class);
        when(xc.createGenerator(any(), any(), any())).thenReturn(generator);
        XContentBuilder builder = new XContentBuilder(xc, os);
        assertNotNull(params.toXContent(builder, null));
    }

}
