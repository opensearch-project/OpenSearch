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
        for (String field : new String[] { "url", "url.original" }) {
            Map<String, Object> config = new HashMap<>();
            config.put("field", field);
            config.put("remove_if_successful", true);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> factory.create(null, null, null, config));
            assertThat(e.getMessage(), containsString("[remove_if_successful]"));
        }
    }

    public void testSameFieldAndTargetFieldIsAllowedWithoutRemoval() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "url");
        UriPartsProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getField(), equalTo("url"));
        assertThat(processor.getTargetField(), equalTo("url"));
    }
}
