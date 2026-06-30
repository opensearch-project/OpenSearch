/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class DataStreamIndexTemplateIT extends DataStreamTestCase {

    public void testCreateDataStreamIndexTemplate() throws Exception {
        // Without the data stream metadata field mapper, data_stream would have been an unknown field in
        // the index template and would have thrown an error.
        createIndexTemplate("demo-template", "{" + "\"index_patterns\": [ \"logs-*\" ]," + "\"data_stream\": { }" + "}");

        // Data stream index template with a custom timestamp field name.
        createIndexTemplate(
            "demo-template",
            "{"
                + "\"index_patterns\": [ \"logs-*\" ],"
                + "\"data_stream\": {"
                + "\"timestamp_field\": { \"name\": \"created_at\" }"
                + "}"
                + "}"
        );
    }

    public void testDeleteIndexTemplate() throws Exception {
        createDataStreamIndexTemplate("demo-template", List.of("logs-*"));
        createDataStream("logs-demo");

        // Index template deletion should fail if there is a data stream using it.
        ExecutionException exception = expectThrows(ExecutionException.class, () -> deleteIndexTemplate("demo-template"));
        assertThat(
            exception.getMessage(),
            containsString("unable to remove composable templates [demo-template] as they are in use by a data streams")
        );

        // Index template can be deleted when all matching data streams are also deleted first.
        deleteDataStreams("logs-demo");
        deleteIndexTemplate("demo-template");
    }

}
