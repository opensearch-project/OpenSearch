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

package org.opensearch.action.admin.indices.mapping.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.indices.datastream.DeleteDataStreamRequestTests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.Strings;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.Index;
import org.opensearch.index.RandomCreateIndexGenerator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.common.collect.Tuple.tuple;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class PutMappingRequestTests extends OpenSearchTestCase {

    public void testValidation() {
        PutMappingRequest r = new PutMappingRequest("myindex");
        ActionRequestValidationException ex = r.validate();

        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is missing"));

        r.source("", XContentType.JSON);
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertTrue(ex.getMessage().contains("source is empty"));

        r.source("somevalidmapping", XContentType.JSON);
        ex = r.validate();
        assertNull("validation should succeed", ex);

        r.setConcreteIndex(new Index("foo", "bar"));
        ex = r.validate();
        assertNotNull("source validation should fail", ex);
        assertEquals(
            ex.getMessage(),
            "Validation Failed: 1: either concrete index or unresolved indices can be set,"
                + " concrete index: [[foo/bar]] and indices: [myindex];"
        );
    }

    /**
     * Test that {@link PutMappingRequest#simpleMapping(String...)}
     * rejects inputs where the {@code Object...} varargs of field name and properties are not
     * paired correctly
     */
    public void testBuildFromSimplifiedDef() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PutMappingRequest.simpleMapping("only_field"));
        assertEquals("mapping source must be pairs of fieldnames and properties definition.", e.getMessage());
    }

    public void testToXContent() throws IOException {
        PutMappingRequest request = new PutMappingRequest("foo");

        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("properties");
        mapping.startObject("email");
        mapping.field("type", "text");
        mapping.endObject();
        mapping.endObject();
        mapping.endObject();
        request.source(mapping);

        String actualRequestBody = Strings.toString(XContentType.JSON, request);
        String expectedRequestBody = "{\"properties\":{\"email\":{\"type\":\"text\"}}}";
        assertEquals(expectedRequestBody, actualRequestBody);
    }

    public void testToXContentWithEmptySource() throws IOException {
        PutMappingRequest request = new PutMappingRequest("foo");

        String actualRequestBody = Strings.toString(XContentType.JSON, request);
        String expectedRequestBody = "{}";
        assertEquals(expectedRequestBody, actualRequestBody);
    }

    public void testToAndFromXContent() throws IOException {

        final PutMappingRequest putMappingRequest = createTestItem();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(putMappingRequest, xContentType, EMPTY_PARAMS, humanReadable);

        PutMappingRequest parsedPutMappingRequest = new PutMappingRequest();
        parsedPutMappingRequest.source(originalBytes, xContentType);

        assertMappingsEqual(putMappingRequest.source(), parsedPutMappingRequest.source());
    }

    private void assertMappingsEqual(String expected, String actual) throws IOException {

        try (
            XContentParser expectedJson = createParser(XContentType.JSON.xContent(), expected);
            XContentParser actualJson = createParser(XContentType.JSON.xContent(), actual)
        ) {
            assertEquals(expectedJson.mapOrdered(), actualJson.mapOrdered());
        }
    }

    /**
     * Returns a random {@link PutMappingRequest}.
     */
    private static PutMappingRequest createTestItem() throws IOException {
        String index = randomAlphaOfLength(5);

        PutMappingRequest request = new PutMappingRequest(index);
        request.source(RandomCreateIndexGenerator.randomMapping());

        return request;
    }

    public void testResolveIndicesWithWriteIndexOnlyAndDataStreamsAndWriteAliases() {
        String[] dataStreamNames = { "foo", "bar", "baz" };
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3))
        );

        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(
            cs,
            List.of(
                tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
                tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
            )
        );
        PutMappingRequest request = new PutMappingRequest().indices("foo", "alias1", "alias2").writeIndexOnly(true);
        Index[] indices = TransportPutMappingAction.resolveIndices(
            cs,
            request,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        // should resolve the data stream and each alias to their respective write indices
        assertThat(indexNames, containsInAnyOrder(expectedDs.getWriteIndex().getIndex().getName(), "index2", "index3"));
    }

    public void testResolveIndicesWithoutWriteIndexOnlyAndDataStreamsAndWriteAliases() {
        String[] dataStreamNames = { "foo", "bar", "baz" };
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3))
        );

        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(
            cs,
            List.of(
                tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
                tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
            )
        );
        PutMappingRequest request = new PutMappingRequest().indices("foo", "alias1", "alias2");
        Index[] indices = TransportPutMappingAction.resolveIndices(
            cs,
            request,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        List<String> expectedIndices = expectedDs.getIndices().stream().map(im -> im.getIndex().getName()).collect(Collectors.toList());
        expectedIndices.addAll(List.of("index1", "index2", "index3"));
        // should resolve the data stream and each alias to _all_ their respective indices
        assertThat(indexNames, containsInAnyOrder(expectedIndices.toArray()));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndDataStreamAndIndex() {
        String[] dataStreamNames = { "foo", "bar", "baz" };
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3))
        );

        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        cs = addAliases(
            cs,
            List.of(
                tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
                tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
            )
        );
        PutMappingRequest request = new PutMappingRequest().indices("foo", "index3").writeIndexOnly(true);
        Index[] indices = TransportPutMappingAction.resolveIndices(
            cs,
            request,
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
        );
        List<String> indexNames = Arrays.stream(indices).map(Index::getName).collect(Collectors.toList());
        IndexAbstraction expectedDs = cs.metadata().getIndicesLookup().get("foo");
        List<String> expectedIndices = expectedDs.getIndices().stream().map(im -> im.getIndex().getName()).collect(Collectors.toList());
        expectedIndices.addAll(List.of("index1", "index2", "index3"));
        // should resolve the data stream and each alias to _all_ their respective indices
        assertThat(indexNames, containsInAnyOrder(expectedDs.getWriteIndex().getIndex().getName(), "index3"));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndNoSingleWriteIndex() {
        String[] dataStreamNames = { "foo", "bar", "baz" };
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3))
        );

        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        final ClusterState cs2 = addAliases(
            cs,
            List.of(
                tuple("alias1", List.of(tuple("index1", false), tuple("index2", true))),
                tuple("alias2", List.of(tuple("index2", false), tuple("index3", true)))
            )
        );
        PutMappingRequest request = new PutMappingRequest().indices("*").writeIndexOnly(true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutMappingAction.resolveIndices(cs2, request, new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)))
        );
        assertThat(e.getMessage(), containsString("The index expression [*] and options provided did not point to a single write-index"));
    }

    public void testResolveIndicesWithWriteIndexOnlyAndAliasWithoutWriteIndex() {
        String[] dataStreamNames = { "foo", "bar", "baz" };
        List<Tuple<String, Integer>> dsMetadata = List.of(
            tuple(dataStreamNames[0], randomIntBetween(1, 3)),
            tuple(dataStreamNames[1], randomIntBetween(1, 3)),
            tuple(dataStreamNames[2], randomIntBetween(1, 3))
        );

        ClusterState cs = DeleteDataStreamRequestTests.getClusterStateWithDataStreams(dsMetadata, List.of("index1", "index2", "index3"));
        final ClusterState cs2 = addAliases(
            cs,
            List.of(
                tuple("alias1", List.of(tuple("index1", false), tuple("index2", false))),
                tuple("alias2", List.of(tuple("index2", false), tuple("index3", false)))
            )
        );
        PutMappingRequest request = new PutMappingRequest().indices("alias2").writeIndexOnly(true);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutMappingAction.resolveIndices(cs2, request, new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)))
        );
        assertThat(e.getMessage(), containsString("no write index is defined for alias [alias2]"));
    }

    /**
     * Adds aliases to the supplied ClusterState instance. The aliases parameter takes of list of tuples of aliasName
     * to the alias's indices. The alias's indices are a tuple of index name and a flag indicating whether the alias
     * is a write alias for that index. See usage examples above.
     */
    private static ClusterState addAliases(ClusterState cs, List<Tuple<String, List<Tuple<String, Boolean>>>> aliases) {
        Metadata.Builder builder = Metadata.builder(cs.metadata());
        for (Tuple<String, List<Tuple<String, Boolean>>> alias : aliases) {
            for (Tuple<String, Boolean> index : alias.v2()) {
                IndexMetadata im = builder.get(index.v1());
                AliasMetadata newAliasMd = AliasMetadata.newAliasMetadataBuilder(alias.v1()).writeIndex(index.v2()).build();
                builder.put(IndexMetadata.builder(im).putAlias(newAliasMd));
            }
        }
        return ClusterState.builder(cs).metadata(builder.build()).build();
    }

}
