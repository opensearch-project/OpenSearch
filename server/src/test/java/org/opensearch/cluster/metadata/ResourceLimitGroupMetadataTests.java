/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.test.AbstractNamedWriteableTestCase;

import java.util.Collections;
import java.util.Set;

import static org.opensearch.cluster.metadata.ResourceLimitGroupTests.createRandomResourceLimitGroup;

public class ResourceLimitGroupMetadataTests extends AbstractNamedWriteableTestCase<ResourceLimitGroupMetadata> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(
                    ResourceLimitGroupMetadata.class,
                    ResourceLimitGroupMetadata.TYPE,
                    ResourceLimitGroupMetadata::new
                )
            )
        );
    }

    @Override
    protected Class<ResourceLimitGroupMetadata> categoryClass() {
        return ResourceLimitGroupMetadata.class;
    }

    @Override
    protected ResourceLimitGroupMetadata createTestInstance() {
        return new ResourceLimitGroupMetadata(getRandomResourceLimitGroups());
    }

    private Set<ResourceLimitGroup> getRandomResourceLimitGroups() {
        return Set.of(createRandomResourceLimitGroup(), createRandomResourceLimitGroup());
    }
}
