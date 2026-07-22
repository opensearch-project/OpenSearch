/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataStreamFieldMapperTests extends OpenSearchSingleNodeTestCase {

    public void testDefaultTimestampField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertDataStreamFieldMapper(mapping, "@timestamp");
    }

    public void testCustomTimestampField() throws Exception {
        String timestampFieldName = "timestamp_" + randomAlphaOfLength(5);

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", timestampFieldName)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertDataStreamFieldMapper(mapping, timestampFieldName);
    }

    public void testDeeplyNestedCustomTimestampField() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .startObject("timestamp_field")
            .field("name", "event.meta.created_at")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        DocumentMapper mapper = createIndex("test").mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("event")
                        .startObject("meta")
                        .field("created_at", "2020-12-06T11:04:05.000Z")
                        .endObject()
                        .endObject()
                        .endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );
        assertThat(doc.rootDoc().getFields("event.meta.created_at").length, equalTo(2));

        MapperException exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "3",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .startObject("event")
                            .startObject("meta")
                            .array("created_at", "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                            .endObject()
                            .endObject()
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field 'event.meta.created_at' of date type")
        );
    }

    public void testEnableOnMappingMergeIsAllowed() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();

        // an index that starts out without the _data_stream_timestamp meta field (the default disabled state)
        mapperService.merge(
            "_doc",
            new CompressedXContent("{\"_doc\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"}}}}"),
            MapperService.MergeReason.MAPPING_UPDATE
        );

        // enabling the meta field on a subsequent merge is allowed (this is how an existing index is adapted into a backing index)
        String enableMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        DocumentMapper merged = mapperService.merge(
            "_doc",
            new CompressedXContent(enableMapping),
            MapperService.MergeReason.MAPPING_UPDATE
        );

        // the meta field is now enabled, so the timestamp validation in postParse is active
        MapperException exception = expectThrows(
            MapperException.class,
            () -> merged.parse(
                new SourceToParse(
                    "test",
                    "1",
                    BytesReference.bytes(XContentFactory.jsonBuilder().startObject().endObject()),
                    MediaTypeRegistry.JSON
                )
            )
        );
        assertThat(exception.getCause().getMessage(), containsString("documents must contain a single-valued timestamp field"));
    }

    public void testDisableOnceEnabledIsRejected() throws Exception {
        MapperService mapperService = createIndex("test").mapperService();

        // start with the meta field enabled
        String enableMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        mapperService.merge("_doc", new CompressedXContent(enableMapping), MapperService.MergeReason.MAPPING_UPDATE);

        // attempting to disable it again on a subsequent merge is rejected by the merge validator
        String disableMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("_data_stream_timestamp")
            .field("enabled", false)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> mapperService.merge("_doc", new CompressedXContent(disableMapping), MapperService.MergeReason.MAPPING_UPDATE)
        );
        assertThat(exception.getMessage(), containsString("Cannot update parameter [enabled] from [true] to [false]"));
    }

    private void assertDataStreamFieldMapper(String mapping, String timestampFieldName) throws Exception {
        DocumentMapper mapper = createIndex("test").mapperService()
            .merge("_doc", new CompressedXContent(mapping), MapperService.MergeReason.MAPPING_UPDATE);

        // Success case - document has timestamp field correctly populated.
        ParsedDocument doc = mapper.parse(
            new SourceToParse(
                "test",
                "1",
                BytesReference.bytes(
                    XContentFactory.jsonBuilder().startObject().field(timestampFieldName, "2020-12-06T11:04:05.000Z").endObject()
                ),
                MediaTypeRegistry.JSON
            )
        );

        // A valid timestamp field will be parsed as LongPoint and SortedNumericDocValuesField.
        assertThat(doc.rootDoc().getFields(timestampFieldName).length, equalTo(2));

        MapperException exception;

        // Failure case - document doesn't have a valid timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "2",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder().startObject().field("invalid-field-name", "2020-12-06T11:04:05.000Z").endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );

        // Failure case - document contains multiple values for the timestamp field.
        exception = expectThrows(MapperException.class, () -> {
            mapper.parse(
                new SourceToParse(
                    "test",
                    "3",
                    BytesReference.bytes(
                        XContentFactory.jsonBuilder()
                            .startObject()
                            .array(timestampFieldName, "2020-12-06T11:04:05.000Z", "2020-12-07T11:04:05.000Z")
                            .endObject()
                    ),
                    MediaTypeRegistry.JSON
                )
            );
        });
        assertThat(
            exception.getCause().getMessage(),
            containsString("documents must contain a single-valued timestamp field '" + timestampFieldName + "' of date type")
        );
    }

}
