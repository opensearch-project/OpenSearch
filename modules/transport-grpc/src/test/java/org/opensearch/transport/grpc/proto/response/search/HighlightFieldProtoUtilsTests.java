/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.core.common.text.Text;
import org.opensearch.protobufs.StringArray;
import org.opensearch.test.OpenSearchTestCase;

public class HighlightFieldProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithEmptyFragments() {
        Text[] emptyFragments = new Text[0];
        StringArray stringArray = HighlightFieldProtoUtils.toProto(emptyFragments);

        assertNotNull("StringArray should not be null", stringArray);
        assertEquals("StringArray should be empty", 0, stringArray.getStringArrayCount());
    }

    public void testToProtoWithSingleFragment() {
        Text[] singleFragment = new Text[] { new Text("highlight fragment") };
        StringArray stringArray = HighlightFieldProtoUtils.toProto(singleFragment);

        assertNotNull("StringArray should not be null", stringArray);
        assertEquals("StringArray should have 1 element", 1, stringArray.getStringArrayCount());
        assertEquals("StringArray element should match", "highlight fragment", stringArray.getStringArray(0));
    }

    public void testToProtoWithMultipleFragments() {
        Text[] multipleFragments = new Text[] {
            new Text("first highlight fragment"),
            new Text("second highlight fragment"),
            new Text("third highlight fragment") };
        StringArray stringArray = HighlightFieldProtoUtils.toProto(multipleFragments);

        assertNotNull("StringArray should not be null", stringArray);
        assertEquals("StringArray should have 3 elements", 3, stringArray.getStringArrayCount());
        assertEquals("First element should match", "first highlight fragment", stringArray.getStringArray(0));
        assertEquals("Second element should match", "second highlight fragment", stringArray.getStringArray(1));
        assertEquals("Third element should match", "third highlight fragment", stringArray.getStringArray(2));
    }

    public void testToProtoWithSpecialCharacters() {
        Text[] specialCharFragments = new Text[] {
            new Text("fragment with <em>HTML</em> tags"),
            new Text("fragment with symbols: !@#$%^&*()"),
            new Text("fragment with unicode: 你好, 世界") };
        StringArray stringArray = HighlightFieldProtoUtils.toProto(specialCharFragments);

        assertNotNull("StringArray should not be null", stringArray);
        assertEquals("StringArray should have 3 elements", 3, stringArray.getStringArrayCount());
        assertEquals("First element should match", "fragment with <em>HTML</em> tags", stringArray.getStringArray(0));
        assertEquals("Second element should match", "fragment with symbols: !@#$%^&*()", stringArray.getStringArray(1));
        assertEquals("Third element should match", "fragment with unicode: 你好, 世界", stringArray.getStringArray(2));
    }
}
