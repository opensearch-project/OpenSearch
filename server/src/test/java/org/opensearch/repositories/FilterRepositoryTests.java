/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterRepositoryTests extends OpenSearchTestCase {

    public void testDoesNotForwardTheDeleteEntrypoint() {
        final Repository inner = mock(Repository.class);
        when(inner.abandonableSnapshotDelete()).thenReturn(Optional.of(mock(Repository.AbandonableSnapshotDelete.class)));

        assertTrue("a filter repository must not forward it", new FilterRepository(inner).abandonableSnapshotDelete().isEmpty());
    }
}
