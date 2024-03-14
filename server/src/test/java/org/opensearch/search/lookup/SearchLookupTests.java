/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.lookup;

import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

public class SearchLookupTests extends OpenSearchTestCase {
    public void testDeprecatedConstructorShardId() {
        final SearchLookup searchLookup = new SearchLookup(mock(MapperService.class), (a, b) -> null);
        assertThrows(IllegalStateException.class, searchLookup::shardId);
    }
}
