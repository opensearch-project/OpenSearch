/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.OpenSearchParseException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class UriPartsProcessorFactoryTests extends OpenSearchTestCase {

    private UriPartsProcessor.Factory factory;

    @Before
    public void init() {
        factory = new UriPartsProcessor.Factory();
    }

    public void testCreateWithDefaults() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "request_url");
        String tag = randomAlphaOfLength(10);

        UriPartsProcessor processor = factory.create(null, tag, null, config);

        assertThat(processor.getType(), equalTo("uri_parts"));
        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getDescription(), nullValue());
        assertThat(processor.getField(), equalTo("request_url"));
        assertThat(processor.getTargetField(), equalTo("url"));
        assertThat(processor.isKeepOriginal(), equalTo(true));
        assertThat(processor.isRemoveIfSuccessful(), equalTo(false));
        assertThat(processor.isIgnoreMissing(), equalTo(false));
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testCreateWithAllOptions() throws Exception {
        boolean keepOriginal = randomBoolean();
        boolean removeIfSuccessful = randomBoolean();
        boolean ignoreMissing = randomBoolean();
        Map<String, Object> config = new HashMap<>();
        config.put("field", "http.request.url");
        config.put("target_field", "parsed.url");
        config.put("keep_original", keepOriginal);
        config.put("remove_if_successful", removeIfSuccessful);
        config.put("ignore_missing", ignoreMissing);
        String tag = randomAlphaOfLength(10);
        String description = randomAlphaOfLength(20);

        UriPartsProcessor processor = factory.create(null, tag, description, config);

        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getDescription(), equalTo(description));
        assertThat(processor.getField(), equalTo("http.request.url"));
        assertThat(processor.getTargetField(), equalTo("parsed.url"));
        assertThat(processor.isKeepOriginal(), equalTo(keepOriginal));
        assertThat(processor.isRemoveIfSuccessful(), equalTo(removeIfSuccessful));
        assertThat(processor.isIgnoreMissing(), equalTo(ignoreMissing));
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testFieldIsRequired() {
        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> factory.create(null, randomAlphaOfLength(10), null, new HashMap<>())
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testExplicitNullOptions() throws Exception {
        Map<String, Object> nullField = new HashMap<>();
        nullField.put("field", null);
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, nullField));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));

        Map<String, Object> nullTarget = new HashMap<>();
        nullTarget.put("field", "request_url");
        nullTarget.put("target_field", null);
        assertThat(factory.create(null, null, null, nullTarget).getTargetField(), equalTo("url"));
    }

    public void testFieldMustBeAString() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", 42);
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), containsString("[field] property isn't a string"));
    }

    public void testEmptyFieldIsRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "");
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[field] cannot be empty"));
    }

    public void testEmptyTargetFieldIsRejected() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "request_url");
        config.put("target_field", "");
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), equalTo("[target_field] cannot be empty"));
    }

    public void testBooleanOptionsMustBeBooleans() {
        for (String option : new String[] { "keep_original", "remove_if_successful", "ignore_missing" }) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", "request_url");
            config.put(option, "maybe");
            OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
            assertThat(e.getMessage(), containsString("[" + option + "] property isn't a boolean"));
        }
    }

    public void testRemoveIfSuccessfulCannotRemoveTheTarget() {
        for (String field : new String[] { "url", "url.original", "_source.url", "_source.url.original" }) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            config.put("remove_if_successful", true);
            // a configuration error, so that it is reported to the client as a 400 and not as an internal error
            OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, null, null, config));
            assertThat(e.getMessage(), containsString("[remove_if_successful]"));
            assertThat(e.getMetadata("opensearch.property_name").get(0), equalTo("remove_if_successful"));
        }
    }

    public void testRemoveIfSuccessfulErrorNamesTheOverlappingFields() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "url.full");
        config.put("target_field", "url");
        config.put("remove_if_successful", true);
        String tag = randomAlphaOfLength(10);
        OpenSearchParseException e = expectThrows(OpenSearchParseException.class, () -> factory.create(null, tag, null, config));
        assertThat(
            e.getMessage(),
            equalTo(
                "[remove_if_successful] cannot be true when [field] [url.full] is the same as, or nested in, [target_field] [url], "
                    + "because the parsed output would be removed"
            )
        );
        assertThat(e.getMetadata("opensearch.processor_type").get(0), equalTo("uri_parts"));
        assertThat(e.getMetadata("opensearch.processor_tag").get(0), equalTo(tag));
    }

    public void testRemoveIfSuccessfulIsRejectedWhenPathsDifferOnlyByTheSourcePrefix() {
        // IngestDocument ignores a leading "_source.", so these pairs address the same field or a field below it
        String[][] overlapping = new String[][] {
            { "_source.url", "url" },
            { "url", "_source.url" },
            { "_source.url", "_source.url" },
            { "_source.url.original", "url" },
            { "url.original", "_source.url" },
            { "_source.url.original", "_source.url" },
            { "_ingest.url", "_ingest.url" },
            { "_ingest.url.original", "_ingest.url" } };
        for (String[] pair : overlapping) {
            OpenSearchParseException e = expectThrows(
                OpenSearchParseException.class,
                () -> factory.create(null, null, null, removalConfig(pair[0], pair[1]))
            );
            assertThat(e.getMessage(), containsString("[remove_if_successful]"));
            assertThat(e.getMessage(), containsString("[field] [" + pair[0] + "]"));
            assertThat(e.getMessage(), containsString("[target_field] [" + pair[1] + "]"));
        }
    }

    public void testRemoveIfSuccessfulIsAllowedForDifferentRoots() throws Exception {
        // "_ingest." addresses the ingest metadata, which is a different root than the source, so nothing can overlap
        String[][] pairs = new String[][] {
            { "_ingest.url", "url" },
            { "url", "_ingest.url" },
            { "_source._ingest.url", "_ingest.url" },
            { "_ingest.url", "_source._ingest.url" },
            { "_source.urls", "url" },
            { "_source.url2", "_source.url" } };
        for (String[] pair : pairs) {
            UriPartsProcessor processor = factory.create(null, null, null, removalConfig(pair[0], pair[1]));
            assertThat(processor.isRemoveIfSuccessful(), equalTo(true));
        }
    }

    public void testRemoveIfSuccessfulIsAllowedForUnrelatedFields() throws Exception {
        String[][] pairs = new String[][] { { "urls", "url" }, { "url", "url2" }, { "a.b", "a.c" } };
        for (String[] pair : pairs) {
            UriPartsProcessor processor = factory.create(null, null, null, removalConfig(pair[0], pair[1]));
            assertThat(processor.isRemoveIfSuccessful(), equalTo(true));
        }
    }

    private static Map<String, Object> removalConfig(String field, String targetField) {
        Map<String, Object> config = new HashMap<>();
        config.put("field", field);
        config.put("target_field", targetField);
        config.put("remove_if_successful", true);
        return config;
    }

    public void testSameFieldAndTargetFieldIsAllowedWithoutRemoval() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "url");
        UriPartsProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("url"));
        assertThat(processor.getTargetField(), equalTo("url"));
    }
}
