/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class ConstantKeywordFieldMapperTests extends OpenSearchSingleNodeTestCase {

    private static final String FIELD_NAME = "field";

    private IndexService indexService;
    private DocumentMapperParser parser;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // When the sandbox stack is installed, its LucenePlugin supplies the real data format, committer and
        // delete engine (each of which the node permits exactly one of), so the in-memory mocks are registered
        // only in the plugin-free run.
        List<Class<? extends Plugin>> plugins = new ArrayList<>(List.of(InternalSettingsPlugin.class));
        if (shouldInstallSandboxStack() == false) {
            plugins.add(MockDataFormatPlugin.class);
            plugins.add(MockCommitterEnginePlugin.class);
        }
        return plugins;
    }

    @Before
    public void setup() {
        indexService = createIndex("test");
        parser = indexService.mapperService().documentMapperParser();
    }

    public void testDefaultDisabledIndexMapper() throws Exception {

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "constant_keyword")
            .field("value", "default_value")
            .endObject()
            .startObject("field2")
            .field("type", "keyword")
            .endObject();
        mapping = mapping.endObject().endObject().endObject();
        DocumentMapper mapper = parser.parse("type", new CompressedXContent(mapping.toString()));

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(b -> {
            b.field("field", "sdf");
            b.field("field2", "szdfvsddf");
        })));
        assertThat(
            e.getMessage(),
            containsString(
                "failed to parse field [field] of type [constant_keyword] in document with id '1'. Preview of field's value: 'sdf'"
            )
        );

        final ParsedDocument doc = mapper.parse(source(b -> {
            b.field("field", "default_value");
            b.field("field2", "field_2_value");
        }));

        final IndexableField field = doc.rootDoc().getField("field");

        // constantKeywordField should not be stored
        assertNull(field);
    }

    public void testMissingDefaultIndexMapper() throws Exception {

        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("field")
            .field("type", "constant_keyword")
            .endObject()
            .startObject("field2")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> parser.parse("type", new CompressedXContent(mapping.toString()))
        );
        assertThat(e.getMessage(), containsString("Field [field] is missing required parameter [value]"));
    }

    public void testBuilderToXContent() throws IOException {
        ConstantKeywordFieldMapper.Builder builder = new ConstantKeywordFieldMapper.Builder("name", "value1");
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder().startObject();
        builder.toXContent(xContentBuilder, false);
        xContentBuilder.endObject();
        assertEquals("{\"value\":\"value1\"}", xContentBuilder.toString());
    }

    private final SourceToParse source(CheckedConsumer<XContentBuilder, IOException> build) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        build.accept(builder);
        builder.endObject();
        return new SourceToParse("test", "1", BytesReference.bytes(builder), MediaTypeRegistry.JSON);
    }

    public void testPossibleToDeriveSource_WhenCopyToPresent() {
        FieldMapper.CopyTo copyTo = new FieldMapper.CopyTo.Builder().add("copy_to_field").build();
        ConstantKeywordFieldMapper mapper = getMapper(copyTo);
        assertThrows(UnsupportedOperationException.class, mapper::canDeriveSource);
    }

    public void testDerivedValueFetching() throws IOException {
        try (Directory directory = newDirectory()) {
            ConstantKeywordFieldMapper mapper = getMapper(FieldMapper.CopyTo.empty());

            try (IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StoredField(FIELD_NAME, "default_value"));
                iw.addDocument(doc);
            }

            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                mapper.deriveSource(builder, reader.leaves().get(0).reader(), 0);
                builder.endObject();
                String source = builder.toString();
                assertEquals("{\"" + FIELD_NAME + "\":" + "\"default_value\"" + "}", source);
            }
        }
    }

    private ConstantKeywordFieldMapper getMapper(FieldMapper.CopyTo copyTo) {
        indexService = createIndexWithSimpleMappings("test-index", Settings.EMPTY, "field", "type=constant_keyword,value=default_value");
        ConstantKeywordFieldMapper mapper = (ConstantKeywordFieldMapper) indexService.mapperService()
            .documentMapper()
            .mappers()
            .getMapper(FIELD_NAME);
        mapper.copyTo = copyTo;
        return mapper;
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testPluggableDataFormatConstantKeywordValidates() throws Exception {
        Settings.Builder indexSettings = Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true);
        if (shouldInstallSandboxStack()) {
            // The in-memory MockDataFormatPlugin cannot be registered alongside the real sandbox stack: both it and
            // the stack's LucenePlugin return a DeleteExecutionEngine, so DataFormatRegistry#getDeleteExecutionEngine
            // would fail its "exactly one DeleteExecutionEngine" check when the shard engine is built. So on the stack
            // path we exercise the real "lucene" data format (LuceneDataFormat.LUCENE_FORMAT_NAME in
            // analytics-backend-lucene, not compile-visible here). No real format (lucene/parquet/composite) declares a
            // "constant_keyword" capability entry, so the pluggable-format capability validation correctly rejects the
            // field and index creation fails with MapperParsingException "... cannot cover ...".
            indexSettings.put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "lucene");
            MapperParsingException e = expectThrows(
                MapperParsingException.class,
                () -> createIndexWithSimpleMappings("test-pluggable", indexSettings.build(), "field", "type=constant_keyword,value=foo")
            );
            assertThat(e.getMessage(), containsString("cannot cover"));
            return;
        }
        // Plugin-free run: the data format value is left unset, so MockDataFormatPlugin's default "" format is selected
        // and capability validation is skipped for the empty format — the constant_keyword field is accepted.
        indexService = createIndexWithSimpleMappings("test-pluggable", indexSettings.build(), "field", "type=constant_keyword,value=foo");
        ConstantKeywordFieldMapper mapper = (ConstantKeywordFieldMapper) indexService.mapperService()
            .documentMapper()
            .mappers()
            .getMapper(FIELD_NAME);
        // parseCreateFieldForPluggableFormat just validates, doesn't addField
        // calling directly since it's protected and same package
        // valid value should not throw
        assertNotNull(mapper);
        // Test via document parse — valid value should succeed
        indexService.mapperService()
            .documentMapper()
            .parse(
                new SourceToParse(
                    "test-pluggable",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().field("field", "foo").endObject()),
                    MediaTypeRegistry.JSON
                ),
                new MockDocumentInput()
            );
    }
}
