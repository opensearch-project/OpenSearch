/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class XContentParserMaxDepthTests extends OpenSearchTestCase {

    public void testDeeplyNestedArrayThrows() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) sb.append('[');
        for (int i = 0; i < 200; i++) sb.append(']');

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sb.toString()
            )
        ) {
            parser.nextToken();
            expectThrows(Exception.class, parser::list);
        }
    }

    public void testDeeplyNestedObjectThrows() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200; i++) sb.append("{\"a\":");
        sb.append("1");
        for (int i = 0; i < 200; i++) sb.append('}');

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sb.toString()
            )
        ) {
            parser.nextToken();
            expectThrows(Exception.class, parser::map);
        }
    }

    public void testModerateNestingSucceeds() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) sb.append('[');
        sb.append("1");
        for (int i = 0; i < 50; i++) sb.append(']');

        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                sb.toString()
            )
        ) {
            parser.nextToken();
            assertNotNull(parser.list());
        }
    }
}
