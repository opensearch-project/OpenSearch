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

import static org.opensearch.cluster.metadata.QueryGroupTests.createRandomQueryGroup;

public class QueryGroupMetadataTests extends AbstractNamedWriteableTestCase<QueryGroupMetadata> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(QueryGroupMetadata.class, QueryGroupMetadata.TYPE, QueryGroupMetadata::new)
            )
        );
    }

    @Override
    protected Class<QueryGroupMetadata> categoryClass() {
        return QueryGroupMetadata.class;
    }

    @Override
    protected QueryGroupMetadata createTestInstance() {
        return new QueryGroupMetadata(getRandomQueryGroups());
    }

    private Set<QueryGroup> getRandomQueryGroups() {
        return Set.of(createRandomQueryGroup(), createRandomQueryGroup());
    }
}
