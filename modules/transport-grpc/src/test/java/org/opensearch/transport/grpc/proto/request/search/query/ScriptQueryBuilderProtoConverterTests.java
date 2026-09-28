/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for ScriptQueryBuilderProtoConverter.
 * Tests only the converter-specific logic (QueryContainer handling).
 * The core conversion logic is tested in ScriptQueryBuilderProtoUtilsTests.
 */
public class ScriptQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private ScriptQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new ScriptQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.SCRIPT, converter.getHandledQueryCase());
    }

    public void testFromProtoWithValidScriptQuery() {
        ScriptQuery scriptQuery = ScriptQuery.newBuilder()
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("true").build()).build())
            .build();

        QueryContainer container = QueryContainer.newBuilder().setScript(scriptQuery).build();

        QueryBuilder result = converter.fromProto(container);

        assertNotNull(result);
        assertTrue(result instanceof org.opensearch.index.query.ScriptQueryBuilder);
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));
        assertEquals("QueryContainer does not contain a Script query", exception.getMessage());
    }

    public void testFromProtoWithContainerWithoutScript() {
        QueryContainer container = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));
        assertEquals("QueryContainer does not contain a Script query", exception.getMessage());
    }
}
