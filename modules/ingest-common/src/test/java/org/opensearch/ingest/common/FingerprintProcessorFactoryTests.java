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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;

public class FingerprintProcessorFactoryTests extends OpenSearchTestCase {

    private FingerprintProcessor.Factory factory;

    @Before
    public void init() {
        factory = new FingerprintProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        List<String> fields = null;
        List<String> excludeFields = null;
        if (randomBoolean()) {
            fields = List.of(randomAlphaOfLength(10));
            config.put("fields", fields);
        } else {
            excludeFields = List.of(randomAlphaOfLength(10));
            config.put("exclude_fields", excludeFields);
        }

        String targetField = null;
        if (randomBoolean()) {
            targetField = randomAlphaOfLength(10);
        }
        config.put("target_field", targetField);

        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);
        String processorTag = randomAlphaOfLength(10);
        FingerprintProcessor fingerprintProcessor = factory.create(null, processorTag, null, config);
        assertThat(fingerprintProcessor.getTag(), equalTo(processorTag));
        assertThat(fingerprintProcessor.getFields(), equalTo(fields));
        assertThat(fingerprintProcessor.getExcludeFields(), equalTo(excludeFields));
        assertThat(fingerprintProcessor.getTargetField(), equalTo(Objects.requireNonNullElse(targetField, "fingerprint")));
        assertThat(fingerprintProcessor.isIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testCreateWithFields() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("fields", List.of(randomAlphaOfLength(10)));
        config.put("exclude_fields", List.of(randomAlphaOfLength(10)));
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[fields] either fields or exclude_fields can be set"));
        }

        config = new HashMap<>();
        List<String> fields = new ArrayList<>();
        if (randomBoolean()) {
            fields.add(null);
        } else {
            fields.add("");
        }
        config.put("fields", fields);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[fields] field name cannot be null nor empty"));
        }

        config = new HashMap<>();
        List<String> excludeFields = new ArrayList<>();
        if (randomBoolean()) {
            excludeFields.add(null);
        } else {
            excludeFields.add("");
        }
        config.put("exclude_fields", excludeFields);
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("[exclude_fields] field name cannot be null nor empty"));
        }
    }

    public void testCreateWithHashMethod() throws Exception {
        Map<String, Object> config = new HashMap<>();
        List<String> fields = List.of(randomAlphaOfLength(10));
        config.put("fields", fields);
        config.put("hash_method", randomAlphaOfLength(10));
        try {
            factory.create(null, null, null, config);
            fail("factory create should have failed");
        } catch (OpenSearchParseException e) {
            assertThat(
                e.getMessage(),
                equalTo("[hash_method] hash method must be MD5@2.16.0, SHA-1@2.16.0, SHA-256@2.16.0 or SHA3-256@2.16.0")
            );
        }
    }
}
