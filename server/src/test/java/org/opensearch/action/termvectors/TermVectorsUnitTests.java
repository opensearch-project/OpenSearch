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

package org.opensearch.action.termvectors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.opensearch.LegacyESVersion;
import org.opensearch.action.termvectors.TermVectorsRequest.Flag;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.action.document.RestTermVectorsAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.StreamsUtils;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class TermVectorsUnitTests extends OpenSearchTestCase {
    public void testStreamResponse() throws Exception {
        TermVectorsResponse outResponse = new TermVectorsResponse("a", "c");
        outResponse.setExists(true);
        writeStandardTermVector(outResponse);

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        TermVectorsResponse inResponse = new TermVectorsResponse(esBuffer);

        // see if correct
        checkIfStandardTermVector(inResponse);

        outResponse = new TermVectorsResponse("a", "c");
        writeEmptyTermVector(outResponse);
        // write
        outBuffer = new ByteArrayOutputStream();
        out = new OutputStreamStreamOutput(outBuffer);
        outResponse.writeTo(out);

        // read
        esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        esBuffer = new InputStreamStreamInput(esInBuffer);
        inResponse = new TermVectorsResponse(esBuffer);
        assertTrue(inResponse.isExists());

    }

    private void writeEmptyTermVector(TermVectorsResponse outResponse) throws IOException {

        Directory dir = newDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());
        conf.setOpenMode(OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);
        FieldType type = new FieldType(TextField.TYPE_STORED);
        type.setStoreTermVectorOffsets(true);
        type.setStoreTermVectorPayloads(false);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectors(true);
        type.freeze();
        Document d = new Document();
        d.add(new Field("id", "abc", StringField.TYPE_STORED));

        writer.updateDocument(new Term("id", "abc"), d);
        writer.commit();
        writer.close();
        DirectoryReader dr = DirectoryReader.open(dir);
        IndexSearcher s = new IndexSearcher(dr);
        TopDocs search = s.search(new TermQuery(new Term("id", "abc")), 1);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        int doc = scoreDocs[0].doc;
        Fields fields = dr.termVectors().get(doc);
        EnumSet<Flag> flags = EnumSet.of(Flag.Positions, Flag.Offsets);
        outResponse.setFields(fields, null, flags, fields);
        outResponse.setExists(true);
        dr.close();
        dir.close();

    }

    private void writeStandardTermVector(TermVectorsResponse outResponse) throws IOException {

        Directory dir = newDirectory();
        IndexWriterConfig conf = new IndexWriterConfig(new StandardAnalyzer());

        conf.setOpenMode(OpenMode.CREATE);
        IndexWriter writer = new IndexWriter(dir, conf);
        FieldType type = new FieldType(TextField.TYPE_STORED);
        type.setStoreTermVectorOffsets(true);
        type.setStoreTermVectorPayloads(false);
        type.setStoreTermVectorPositions(true);
        type.setStoreTermVectors(true);
        type.freeze();
        Document d = new Document();
        d.add(new Field("id", "abc", StringField.TYPE_STORED));
        d.add(new Field("title", "the1 quick brown fox jumps over  the1 lazy dog", type));
        d.add(new Field("desc", "the1 quick brown fox jumps over  the1 lazy dog", type));

        writer.updateDocument(new Term("id", "abc"), d);
        writer.commit();
        writer.close();
        DirectoryReader dr = DirectoryReader.open(dir);
        IndexSearcher s = new IndexSearcher(dr);
        TopDocs search = s.search(new TermQuery(new Term("id", "abc")), 1);
        ScoreDoc[] scoreDocs = search.scoreDocs;
        int doc = scoreDocs[0].doc;
        Fields termVectors = dr.termVectors().get(doc);
        EnumSet<Flag> flags = EnumSet.of(Flag.Positions, Flag.Offsets);
        outResponse.setFields(termVectors, null, flags, termVectors);
        dr.close();
        dir.close();

    }

    private void checkIfStandardTermVector(TermVectorsResponse inResponse) throws IOException {

        Fields fields = inResponse.getFields();
        assertThat(fields.terms("title"), Matchers.notNullValue());
        assertThat(fields.terms("desc"), Matchers.notNullValue());
        assertThat(fields.size(), equalTo(2));
    }

    public void testRestRequestParsing() throws Exception {
        BytesReference inputBytes = new BytesArray(
            " {\"fields\" : [\"a\",  \"b\",\"c\"], \"offsets\":false, \"positions\":false, \"payloads\":true}"
        );

        TermVectorsRequest tvr = new TermVectorsRequest(null, null);
        XContentParser parser = createParser(JsonXContent.jsonXContent, inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser);

        Set<String> fields = tvr.selectedFields();
        assertThat(fields.contains("a"), equalTo(true));
        assertThat(fields.contains("b"), equalTo(true));
        assertThat(fields.contains("c"), equalTo(true));
        assertThat(tvr.offsets(), equalTo(false));
        assertThat(tvr.positions(), equalTo(false));
        assertThat(tvr.payloads(), equalTo(true));
        String additionalFields = "b,c  ,d, e  ";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields().size(), equalTo(5));
        assertThat(fields.contains("d"), equalTo(true));
        assertThat(fields.contains("e"), equalTo(true));

        additionalFields = "";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);

        inputBytes = new BytesArray(" {\"offsets\":false, \"positions\":false, \"payloads\":true}");
        tvr = new TermVectorsRequest(null, null);
        parser = createParser(JsonXContent.jsonXContent, inputBytes);
        TermVectorsRequest.parseRequest(tvr, parser);
        additionalFields = "";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields(), equalTo(null));
        additionalFields = "b,c  ,d, e  ";
        RestTermVectorsAction.addFieldStringsFromParameter(tvr, additionalFields);
        assertThat(tvr.selectedFields().size(), equalTo(4));

    }

    public void testRequestParsingThrowsException() {
        BytesReference inputBytes = new BytesArray(
            " {\"fields\" : \"a,  b,c   \", \"offsets\":false, \"positions\":false, \"payloads\":true, \"meaningless_term\":2}"
        );
        TermVectorsRequest tvr = new TermVectorsRequest(null, null);
        boolean threwException = false;
        try {
            XContentParser parser = createParser(JsonXContent.jsonXContent, inputBytes);
            TermVectorsRequest.parseRequest(tvr, parser);
        } catch (Exception e) {
            threwException = true;
        }
        assertThat(threwException, equalTo(true));

    }

    public void testStreamRequest() throws IOException {
        for (int i = 0; i < 10; i++) {
            TermVectorsRequest request = new TermVectorsRequest("index", "id");
            request.offsets(random().nextBoolean());
            request.fieldStatistics(random().nextBoolean());
            request.payloads(random().nextBoolean());
            request.positions(random().nextBoolean());
            request.termStatistics(random().nextBoolean());
            String pref = random().nextBoolean() ? "somePreference" : null;
            request.preference(pref);
            request.doc(new BytesArray("{}"), randomBoolean(), MediaTypeRegistry.JSON);

            // write
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            request.writeTo(out);

            // read
            ByteArrayInputStream opensearchInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput opensearchBuffer = new InputStreamStreamInput(opensearchInBuffer);
            TermVectorsRequest req2 = new TermVectorsRequest(opensearchBuffer);

            assertThat(request.offsets(), equalTo(req2.offsets()));
            assertThat(request.fieldStatistics(), equalTo(req2.fieldStatistics()));
            assertThat(request.payloads(), equalTo(req2.payloads()));
            assertThat(request.positions(), equalTo(req2.positions()));
            assertThat(request.termStatistics(), equalTo(req2.termStatistics()));
            assertThat(request.preference(), equalTo(pref));
            assertThat(request.routing(), equalTo(null));
            assertEquals(new BytesArray("{}"), request.doc());
            assertEquals(MediaTypeRegistry.JSON, request.xContentType());
        }
    }

    public void testStreamRequestLegacyVersion() throws IOException {
        for (int i = 0; i < 10; i++) {
            TermVectorsRequest request = new TermVectorsRequest("index", "id");
            request.offsets(random().nextBoolean());
            request.fieldStatistics(random().nextBoolean());
            request.payloads(random().nextBoolean());
            request.positions(random().nextBoolean());
            request.termStatistics(random().nextBoolean());
            String pref = random().nextBoolean() ? "somePreference" : null;
            request.preference(pref);
            request.doc(new BytesArray("{}"), randomBoolean(), MediaTypeRegistry.JSON);

            // write using older version which contains types
            ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
            OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
            out.setVersion(LegacyESVersion.fromId(7000099));
            request.writeTo(out);

            // First check the type on the stream was written as "_doc" by manually parsing the stream until the type
            ByteArrayInputStream opensearchInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            InputStreamStreamInput opensearchBuffer = new InputStreamStreamInput(opensearchInBuffer);
            TaskId.readFromStream(opensearchBuffer);
            if (opensearchBuffer.readBoolean()) {
                new ShardId(opensearchBuffer);
            }
            opensearchBuffer.readOptionalString();
            assertThat(opensearchBuffer.readString(), equalTo("_doc"));

            // now read the stream as normal to check it is parsed correct if received from an older node
            opensearchInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
            opensearchBuffer = new InputStreamStreamInput(opensearchInBuffer);
            opensearchBuffer.setVersion(LegacyESVersion.fromId(7000099));
            TermVectorsRequest req2 = new TermVectorsRequest(opensearchBuffer);

            assertThat(request.offsets(), equalTo(req2.offsets()));
            assertThat(request.fieldStatistics(), equalTo(req2.fieldStatistics()));
            assertThat(request.payloads(), equalTo(req2.payloads()));
            assertThat(request.positions(), equalTo(req2.positions()));
            assertThat(request.termStatistics(), equalTo(req2.termStatistics()));
            assertThat(request.preference(), equalTo(pref));
            assertThat(request.routing(), equalTo(null));
            assertEquals(new BytesArray("{}"), request.doc());
            assertEquals(MediaTypeRegistry.JSON, request.xContentType());
        }
    }

    public void testMultiParser() throws Exception {
        byte[] bytes = StreamsUtils.copyToBytesFromClasspath("/org/opensearch/action/termvectors/multiRequest1.json");
        XContentParser data = createParser(JsonXContent.jsonXContent, bytes);
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);
        checkParsedParameters(request);

        bytes = StreamsUtils.copyToBytesFromClasspath("/org/opensearch/action/termvectors/multiRequest2.json");
        data = createParser(JsonXContent.jsonXContent, new BytesArray(bytes));
        request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);

        checkParsedParameters(request);
    }

    void checkParsedParameters(MultiTermVectorsRequest request) {
        Set<String> ids = new HashSet<>();
        ids.add("1");
        ids.add("2");
        Set<String> fields = new HashSet<>();
        fields.add("a");
        fields.add("b");
        fields.add("c");
        for (TermVectorsRequest singleRequest : request.requests) {
            assertThat(singleRequest.index(), equalTo("testidx"));
            assertThat(singleRequest.payloads(), equalTo(false));
            assertThat(singleRequest.positions(), equalTo(false));
            assertThat(singleRequest.offsets(), equalTo(false));
            assertThat(singleRequest.termStatistics(), equalTo(true));
            assertThat(singleRequest.fieldStatistics(), equalTo(false));
            assertThat(singleRequest.id(), Matchers.anyOf(Matchers.equalTo("1"), Matchers.equalTo("2")));
            assertThat(singleRequest.selectedFields(), equalTo(fields));
        }
    }

    // issue #12311
    public void testMultiParserFilter() throws Exception {
        byte[] bytes = StreamsUtils.copyToBytesFromClasspath("/org/opensearch/action/termvectors/multiRequest3.json");
        XContentParser data = createParser(JsonXContent.jsonXContent, bytes);
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        request.add(new TermVectorsRequest(), data);

        checkParsedFilterParameters(request);
    }

    void checkParsedFilterParameters(MultiTermVectorsRequest multiRequest) {
        Set<String> ids = new HashSet<>(Arrays.asList("1", "2"));
        for (TermVectorsRequest request : multiRequest.requests) {
            assertThat(request.index(), equalTo("testidx"));
            assertTrue(ids.remove(request.id()));
            assertNotNull(request.filterSettings());
            assertThat(request.filterSettings().maxNumTerms, equalTo(20));
            assertThat(request.filterSettings().minTermFreq, equalTo(1));
            assertThat(request.filterSettings().maxTermFreq, equalTo(20));
            assertThat(request.filterSettings().minDocFreq, equalTo(1));
            assertThat(request.filterSettings().maxDocFreq, equalTo(20));
            assertThat(request.filterSettings().minWordLength, equalTo(1));
            assertThat(request.filterSettings().maxWordLength, equalTo(20));
        }
        assertTrue(ids.isEmpty());
    }
}
