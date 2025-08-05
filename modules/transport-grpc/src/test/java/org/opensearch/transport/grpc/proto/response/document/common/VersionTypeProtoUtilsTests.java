/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.document.common;

import org.opensearch.index.VersionType;
import org.opensearch.test.OpenSearchTestCase;

public class VersionTypeProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithVersionTypeExternal() {
        // Test conversion from VersionType.VERSION_TYPE_EXTERNAL to VersionType.EXTERNAL
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL);

        // Verify the result
        assertEquals("VERSION_TYPE_EXTERNAL should convert to VersionType.EXTERNAL", VersionType.EXTERNAL, result);
    }

    public void testFromProtoWithVersionTypeExternalGte() {
        // Test conversion from VersionType.VERSION_TYPE_EXTERNAL_GTE to VersionType.EXTERNAL_GTE
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL_GTE);

        // Verify the result
        assertEquals("VERSION_TYPE_EXTERNAL_GTE should convert to VersionType.EXTERNAL_GTE", VersionType.EXTERNAL_GTE, result);
    }

    public void testFromProtoWithDefaultCase() {
        // Test conversion with a default case (should return INTERNAL)
        // Using UNSPECIFIED which will hit the default case
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_UNSPECIFIED);

        // Verify the result
        assertEquals("Default case should convert to VersionType.INTERNAL", VersionType.INTERNAL, result);
    }

    public void testFromProtoWithUnrecognizedVersionType() {
        // Test conversion with an unrecognized VersionType
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.UNRECOGNIZED);

        // Verify the result (should default to INTERNAL)
        assertEquals("UNRECOGNIZED should default to VersionType.INTERNAL", VersionType.INTERNAL, result);
    }
}
