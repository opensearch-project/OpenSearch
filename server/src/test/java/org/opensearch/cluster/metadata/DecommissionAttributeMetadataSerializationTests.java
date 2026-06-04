/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;

public class DecommissionAttributeMetadataSerializationTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return DecommissionAttributeMetadata::new;
    }

    @Override
    protected Metadata.Custom createTestInstance() {
        String attributeName = randomAlphaOfLength(6);
        String attributeValue = randomAlphaOfLength(6);
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute(attributeName, attributeValue);
        DecommissionStatus decommissionStatus = randomFrom(DecommissionStatus.values());
        return new DecommissionAttributeMetadata(decommissionAttribute, decommissionStatus, randomAlphaOfLength(10));
    }

    @Override
    protected Metadata.Custom mutateInstance(Metadata.Custom instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {
        DecommissionAttributeMetadata decommissionAttributeMetadata = (DecommissionAttributeMetadata) testInstance;
        DecommissionAttribute decommissionAttribute = decommissionAttributeMetadata.decommissionAttribute();
        String attributeName = decommissionAttribute.attributeName();
        String attributeValue = decommissionAttribute.attributeValue();
        DecommissionStatus decommissionStatus = decommissionAttributeMetadata.status();
        if (randomBoolean()) {
            decommissionStatus = randomFrom(DecommissionStatus.values());
        }
        if (randomBoolean()) {
            attributeName = randomAlphaOfLength(6);
        }
        if (randomBoolean()) {
            attributeValue = randomAlphaOfLength(6);
        }
        return new DecommissionAttributeMetadata(
            new DecommissionAttribute(attributeName, attributeValue),
            decommissionStatus,
            randomAlphaOfLength(10)
        );
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return DecommissionAttributeMetadata::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Metadata.Custom doParseInstance(XContentParser parser) throws IOException {
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        DecommissionAttributeMetadata decommissionAttributeMetadata = DecommissionAttributeMetadata.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        return new DecommissionAttributeMetadata(
            decommissionAttributeMetadata.decommissionAttribute(),
            decommissionAttributeMetadata.status(),
            decommissionAttributeMetadata.requestID()
        );
    }
}
