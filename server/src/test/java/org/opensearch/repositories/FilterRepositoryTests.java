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

/**
 * Tests that {@link FilterRepository} does not forward the delete entrypoint of the repository it decorates. A decorator
 * that wants budgeted deletes overrides the accessor and maps the wrapped entrypoint itself, passing the same attempt; one
 * that does not is deleted through the narrow overload, which reaches its own overrides.
 */
public class FilterRepositoryTests extends OpenSearchTestCase {

    public void testDoesNotForwardTheDeleteEntrypoint() {
        final Repository inner = mock(Repository.class);
        final Repository.AbandonableSnapshotDelete entrypoint = (
            snapshotIds,
            repositoryStateId,
            repositoryMetaVersion,
            deletion,
            listener) -> {};
        when(inner.abandonableSnapshotDelete()).thenReturn(Optional.of(entrypoint));

        assertTrue("a filter repository must not forward it", new FilterRepository(inner).abandonableSnapshotDelete().isEmpty());
    }
}
