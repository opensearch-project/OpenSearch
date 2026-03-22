/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.script.ContextAwareGroupingScript;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContextAwareGroupingFieldMapperTests extends OpenSearchTestCase {

    private ScriptService mockScriptService;
    private Mapper.TypeParser.ParserContext mockParserContext;
    private ObjectMapper.Builder mockObjectMapperBuilder;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Set up mocks for dependencies
        mockScriptService = mock(ScriptService.class);
        mockParserContext = new Mapper.TypeParser.ParserContext(null, null, null, null, null, null, mockScriptService);
        mockObjectMapperBuilder = new ObjectMapper.Builder("parent_field");
    }

    public void testValidFieldsMapping() throws Exception {
        // Add a field to the mock ObjectMapper.Builder to satisfy the validation
        mockObjectMapperBuilder.add(new KeywordFieldMapper.Builder("keyword_field"));

        Map<String, Object> node = new HashMap<>();
        List<String> fields = Collections.singletonList("keyword_field");
        node.put("fields", fields);

        ContextAwareGroupingFieldMapper.Builder builder = (ContextAwareGroupingFieldMapper.Builder) ContextAwareGroupingFieldMapper.PARSER
            .parse("context_aware_grouping", node, mockParserContext, mockObjectMapperBuilder);

        ContextAwareGroupingFieldMapper mapper = (ContextAwareGroupingFieldMapper) builder.build(
            new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0))
        );

        assertEquals("context_aware_grouping", mapper.simpleName());
        assertEquals(fields, mapper.fieldType().fields());
        assertNull(mapper.fieldType().compiledScript());
    }

    public void testInvalidFieldsType() {
        // Not a list
        Map<String, Object> node1 = new HashMap<>();
        node1.put("fields", "not_a_list");

        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> ContextAwareGroupingFieldMapper.PARSER.parse("context_aware_grouping", node1, mockParserContext, mockObjectMapperBuilder)
        );
        assertTrue(e.getMessage().contains("Expected [fields] to be a list of strings but got [not_a_list]"));

        // Multiple fields
        Map<String, Object> node2 = new HashMap<>();
        List<String> fields = List.of("keyword_field", "multi_field");
        node2.put("fields", fields);

        e = expectThrows(
            MapperParsingException.class,
            () -> ContextAwareGroupingFieldMapper.PARSER.parse("context_aware_grouping", node2, mockParserContext, mockObjectMapperBuilder)
        );
        assertTrue(e.getMessage().contains("Currently [fields] in context_aware_grouping does not support multiple values"));

        // No fields
        e = expectThrows(
            MapperParsingException.class,
            () -> ContextAwareGroupingFieldMapper.PARSER.parse(
                "context_aware_grouping",
                emptyMap(),
                mockParserContext,
                mockObjectMapperBuilder
            )
        );
        assertTrue(e.getMessage().contains("[fields] in context_aware_grouping is required"));

        // fields not present in properties of mapping
        mockObjectMapperBuilder.add(new KeywordFieldMapper.Builder("keyword_field"));

        Map<String, Object> node = new HashMap<>();
        node.put("fields", Collections.singletonList("blah"));

        e = expectThrows(
            MapperParsingException.class,
            () -> ContextAwareGroupingFieldMapper.PARSER.parse("context_aware_grouping", node, mockParserContext, mockObjectMapperBuilder)
        );
    }

    public void testValidScriptMapping() {
        mockObjectMapperBuilder.add(new KeywordFieldMapper.Builder("keyword_field"));
        Map<String, Object> node = new HashMap<>();
        Map<String, Object> scriptNode = new HashMap<>();
        scriptNode.put("source", "return \"hello\";");
        node.put("fields", singletonList("keyword_field"));
        node.put("script", scriptNode);

        ContextAwareGroupingScript.Factory mockFactory = mock(ContextAwareGroupingScript.Factory.class);
        ContextAwareGroupingScript mockScript = mock(ContextAwareGroupingScript.class);

        when(mockScriptService.compile(any(Script.class), eq(ContextAwareGroupingScript.CONTEXT))).thenReturn(mockFactory);
        when(mockFactory.newInstance()).thenReturn(mockScript);

        ContextAwareGroupingFieldMapper.Builder builder = (ContextAwareGroupingFieldMapper.Builder) ContextAwareGroupingFieldMapper.PARSER
            .parse("context_aware_grouping", node, mockParserContext, mockObjectMapperBuilder);

        ContextAwareGroupingFieldMapper mapper = (ContextAwareGroupingFieldMapper) builder.build(
            new Mapper.BuilderContext(Settings.EMPTY, new ContentPath(0))
        );

        assertNotNull(mapper.fieldType().compiledScript());
    }

    public void testIngestAttemptThrowsException() {
        ContextAwareGroupingFieldType fieldType = new ContextAwareGroupingFieldType(Collections.emptyList(), null);
        ContextAwareGroupingFieldMapper mapper = new ContextAwareGroupingFieldMapper(
            "context_aware_grouping",
            fieldType,
            new ContextAwareGroupingFieldMapper.Builder("context_aware_grouping")
        );

        // Mock a ParseContext
        ParseContext mockParseContext = mock(ParseContext.class);

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parseCreateField(mockParseContext));
        assertTrue(e.getMessage().contains("context_aware_grouping cannot be ingested in the document"));
    }

    public void testContextAwareFieldMapperWithDerivedSource() throws IOException {
        ContextAwareGroupingFieldType fieldType = new ContextAwareGroupingFieldType(Collections.emptyList(), null);
        ContextAwareGroupingFieldMapper mapper = new ContextAwareGroupingFieldMapper(
            "context_aware_grouping",
            fieldType,
            new ContextAwareGroupingFieldMapper.Builder("context_aware_grouping")
        );
        LeafReader leafReader = mock(LeafReader.class);

        try {
            mapper.canDeriveSource();
            mapper.deriveSource(XContentFactory.jsonBuilder().startObject(), leafReader, 0);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
