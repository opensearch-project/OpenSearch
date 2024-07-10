/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractRemoteWritableEntityManagerTests extends OpenSearchTestCase {
    public void testGetStoreWithKnownEntityType() {
        AbstractRemoteWritableEntityManager manager = new ConcreteRemoteWritableEntityManager();
        String knownEntityType = "knownType";
        RemoteWritableEntityStore mockStore = mock(RemoteWritableEntityStore.class);
        manager.remoteWritableEntityStores.put(knownEntityType, mockStore);
        AbstractRemoteWritableBlobEntity mockEntity = mock(AbstractRemoteWritableBlobEntity.class);
        when(mockEntity.getType()).thenReturn(knownEntityType);

        RemoteWritableEntityStore store = manager.getStore(mockEntity);
        verify(mockEntity).getType();
        assertEquals(mockStore, store);
    }

    public void testGetStoreWithUnknownEntityType() {
        AbstractRemoteWritableEntityManager manager = new ConcreteRemoteWritableEntityManager();
        String unknownEntityType = "unknownType";
        AbstractRemoteWritableBlobEntity mockEntity = mock(AbstractRemoteWritableBlobEntity.class);
        when(mockEntity.getType()).thenReturn(unknownEntityType);

        assertThrows(IllegalArgumentException.class, () -> manager.getStore(mockEntity));
        verify(mockEntity, times(2)).getType();
    }

    private static class ConcreteRemoteWritableEntityManager extends AbstractRemoteWritableEntityManager {
        @Override
        protected ActionListener<Void> getWriteActionListener(
            String component,
            AbstractRemoteWritableBlobEntity remoteObject,
            ActionListener<ClusterMetadataManifest.UploadedMetadata> listener
        ) {
            return null;
        }

        @Override
        protected ActionListener<Object> getReadActionListener(
            String component,
            AbstractRemoteWritableBlobEntity remoteObject,
            ActionListener<RemoteReadResult> listener
        ) {
            return null;
        }
    }
}
