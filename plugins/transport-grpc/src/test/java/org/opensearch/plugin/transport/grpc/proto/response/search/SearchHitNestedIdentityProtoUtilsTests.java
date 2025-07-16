/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.search;

import org.opensearch.protobufs.NestedIdentity;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchTestCase;

public class SearchHitNestedIdentityProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithBasicNestedIdentity() throws Exception {
        // Create a SearchHit.NestedIdentity with basic fields
        SearchHit.NestedIdentity nestedIdentity = new SearchHit.NestedIdentity("parent_field", 5, null);

        // Call the method under test
        NestedIdentity protoNestedIdentity = SearchHitProtoUtils.NestedIdentityProtoUtils.toProto(nestedIdentity);

        // Verify the result
        assertNotNull("NestedIdentity should not be null", protoNestedIdentity);
        assertEquals("Field should match", "parent_field", protoNestedIdentity.getField());
        assertEquals("Offset should match", 5, protoNestedIdentity.getOffset());
        assertFalse("Nested field should not be set", protoNestedIdentity.hasNested());
    }

    public void testToProtoWithNestedNestedIdentity() throws Exception {
        // Create a nested SearchHit.NestedIdentity
        SearchHit.NestedIdentity childNestedIdentity = new SearchHit.NestedIdentity("child_field", 2, null);
        SearchHit.NestedIdentity parentNestedIdentity = new SearchHit.NestedIdentity("parent_field", 5, childNestedIdentity);

        // Call the method under test
        NestedIdentity protoNestedIdentity = SearchHitProtoUtils.NestedIdentityProtoUtils.toProto(parentNestedIdentity);

        // Verify the result
        assertNotNull("NestedIdentity should not be null", protoNestedIdentity);
        assertEquals("Field should match", "parent_field", protoNestedIdentity.getField());
        assertEquals("Offset should match", 5, protoNestedIdentity.getOffset());
        assertTrue("Nested field should be set", protoNestedIdentity.hasNested());

        // Verify the nested identity
        NestedIdentity nestedProtoNestedIdentity = protoNestedIdentity.getNested();
        assertNotNull("Nested NestedIdentity should not be null", nestedProtoNestedIdentity);
        assertEquals("Nested field should match", "child_field", nestedProtoNestedIdentity.getField());
        assertEquals("Nested offset should match", 2, nestedProtoNestedIdentity.getOffset());
        assertFalse("Nested nested field should not be set", nestedProtoNestedIdentity.hasNested());
    }

    public void testToProtoWithDeeplyNestedNestedIdentity() throws Exception {
        // Create a deeply nested SearchHit.NestedIdentity
        SearchHit.NestedIdentity grandchildNestedIdentity = new SearchHit.NestedIdentity("grandchild_field", 1, null);
        SearchHit.NestedIdentity childNestedIdentity = new SearchHit.NestedIdentity("child_field", 2, grandchildNestedIdentity);
        SearchHit.NestedIdentity parentNestedIdentity = new SearchHit.NestedIdentity("parent_field", 5, childNestedIdentity);

        // Call the method under test
        NestedIdentity protoNestedIdentity = SearchHitProtoUtils.NestedIdentityProtoUtils.toProto(parentNestedIdentity);

        // Verify the result
        assertNotNull("NestedIdentity should not be null", protoNestedIdentity);
        assertEquals("Field should match", "parent_field", protoNestedIdentity.getField());
        assertEquals("Offset should match", 5, protoNestedIdentity.getOffset());
        assertTrue("Nested field should be set", protoNestedIdentity.hasNested());

        // Verify the child nested identity
        NestedIdentity childProtoNestedIdentity = protoNestedIdentity.getNested();
        assertNotNull("Child NestedIdentity should not be null", childProtoNestedIdentity);
        assertEquals("Child field should match", "child_field", childProtoNestedIdentity.getField());
        assertEquals("Child offset should match", 2, childProtoNestedIdentity.getOffset());
        assertTrue("Child nested field should be set", childProtoNestedIdentity.hasNested());

        // Verify the grandchild nested identity
        NestedIdentity grandchildProtoNestedIdentity = childProtoNestedIdentity.getNested();
        assertNotNull("Grandchild NestedIdentity should not be null", grandchildProtoNestedIdentity);
        assertEquals("Grandchild field should match", "grandchild_field", grandchildProtoNestedIdentity.getField());
        assertEquals("Grandchild offset should match", 1, grandchildProtoNestedIdentity.getOffset());
        assertFalse("Grandchild nested field should not be set", grandchildProtoNestedIdentity.hasNested());
    }

    public void testToProtoWithNegativeOffset() throws Exception {
        // Create a SearchHit.NestedIdentity with negative offset
        SearchHit.NestedIdentity nestedIdentity = new SearchHit.NestedIdentity("field", -1, null);

        // Call the method under test
        NestedIdentity protoNestedIdentity = SearchHitProtoUtils.NestedIdentityProtoUtils.toProto(nestedIdentity);

        // Verify the result
        assertNotNull("NestedIdentity should not be null", protoNestedIdentity);
        assertEquals("Field should match", "field", protoNestedIdentity.getField());
        assertEquals("Offset should not be set", 0, protoNestedIdentity.getOffset());
        assertFalse("Nested field should not be set", protoNestedIdentity.hasNested());
    }
}
