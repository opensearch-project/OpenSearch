/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AdvancedSettingsResponseTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("theme:darkMode", true);
        settings.put("dateFormat", "YYYY-MM-DD");
        settings.put("defaultIndex", "logs-2.19.5");

        AdvancedSettingsResponse response = new AdvancedSettingsResponse(settings);

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AdvancedSettingsResponse deserialized = new AdvancedSettingsResponse(input);

        assertEquals(settings, deserialized.getSettings());
    }

    public void testSerializationWithEmptySettings() throws IOException {
        AdvancedSettingsResponse response = new AdvancedSettingsResponse(new HashMap<>());

        BytesStreamOutput output = new BytesStreamOutput();
        response.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AdvancedSettingsResponse deserialized = new AdvancedSettingsResponse(input);

        assertTrue(deserialized.getSettings().isEmpty());
    }

    public void testToXContent() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("theme:darkMode", true);
        settings.put("dateFormat", "YYYY-MM-DD");

        AdvancedSettingsResponse response = new AdvancedSettingsResponse(settings);

        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertTrue(json.contains("\"theme:darkMode\":true"));
        assertTrue(json.contains("\"dateFormat\":\"YYYY-MM-DD\""));
    }

    public void testToXContentWithNullSettings() throws IOException {
        AdvancedSettingsResponse response = new AdvancedSettingsResponse((Map<String, Object>) null);

        XContentBuilder builder = JsonXContent.contentBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.flush();

        String json = BytesReference.bytes(builder).utf8ToString();
        assertEquals("{}", json);
    }
}
