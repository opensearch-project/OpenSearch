/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Set;

public class SecurityAttributeTests extends OpenSearchTestCase {

    public void testGetName() {
        SecurityAttribute attribute = SecurityAttribute.PRINCIPAL;
        assertEquals("principal", attribute.getName());
    }

    public void testFromXContent() throws IOException {
        String json = """
            {
              "username": ["alice", "bob"],
              "role": ["admin"]
            }
            """;

        XContentParser parser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(json),
            XContentType.JSON
        );

        parser.nextToken();
        Set<String> result = SecurityAttribute.PRINCIPAL.fromXContentParseAttributeValues(parser);
        assertTrue(result.contains("username_alice"));
        assertTrue(result.contains("username_bob"));
        assertTrue(result.contains("role_admin"));
    }

    public void testToXContent() throws IOException {
        Set<String> input = Set.of("username_alice", "username_bob_lastname", "role_admin_admin");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        SecurityAttribute.PRINCIPAL.toXContentWriteAttributeValues(builder, input);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("alice"));
        assertTrue(json.contains("bob_lastname"));
        assertTrue(json.contains("admin_admin"));
    }
}
