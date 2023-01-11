/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class DecommissionAttributeMetadataXContentTests extends AbstractXContentTestCase<DecommissionAttributeMetadata> {
    @Override
    protected DecommissionAttributeMetadata createTestInstance() {
        String attributeName = randomAlphaOfLength(6);
        String attributeValue = randomAlphaOfLength(6);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        DecommissionStatus decommissionStatus = randomFrom(DecommissionStatus.values());
        return new DecommissionAttributeMetadata(decommissionAttribute, decommissionStatus, randomAlphaOfLength(10));
    }

    @Override
    protected DecommissionAttributeMetadata doParseInstance(XContentParser parser) throws IOException {
        return DecommissionAttributeMetadata.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
