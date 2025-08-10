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
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL);

        assertEquals("VERSION_TYPE_EXTERNAL should convert to VersionType.EXTERNAL", VersionType.EXTERNAL, result);
    }

    public void testFromProtoWithVersionTypeExternalGte() {
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_EXTERNAL_GTE);

        assertEquals("VERSION_TYPE_EXTERNAL_GTE should convert to VersionType.EXTERNAL_GTE", VersionType.EXTERNAL_GTE, result);
    }

    public void testFromProtoWithDefaultCase() {
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.VERSION_TYPE_INTERNAL);

        assertEquals("Default case should convert to VersionType.INTERNAL", VersionType.INTERNAL, result);
    }

    public void testFromProtoWithUnrecognizedVersionType() {
        VersionType result = VersionTypeProtoUtils.fromProto(org.opensearch.protobufs.VersionType.UNRECOGNIZED);

        assertEquals("UNRECOGNIZED should default to VersionType.INTERNAL", VersionType.INTERNAL, result);
    }
}
