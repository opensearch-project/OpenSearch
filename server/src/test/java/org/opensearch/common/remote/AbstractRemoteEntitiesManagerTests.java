/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.model.RemoteReadResult;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractRemoteEntitiesManagerTests extends OpenSearchTestCase {
    public void testGetStoreWithKnownEntityType() {
        AbstractRemoteEntitiesManager manager = new ConcreteRemoteEntitiesManager();
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
        AbstractRemoteEntitiesManager manager = new ConcreteRemoteEntitiesManager();
        String unknownEntityType = "unknownType";
        AbstractRemoteWritableBlobEntity mockEntity = mock(AbstractRemoteWritableBlobEntity.class);
        when(mockEntity.getType()).thenReturn(unknownEntityType);

        assertThrows(IllegalArgumentException.class, () -> manager.getStore(mockEntity));
        verify(mockEntity, times(2)).getType();
    }

    private static class ConcreteRemoteEntitiesManager extends AbstractRemoteEntitiesManager {
        @Override
        protected ActionListener<Void> getWriteActionListener(
            String component,
            AbstractRemoteWritableBlobEntity remoteObject,
            LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
        ) {
            return null;
        }

        @Override
        protected ActionListener<Object> getReadActionListener(
            String component,
            AbstractRemoteWritableBlobEntity remoteObject,
            LatchedActionListener<RemoteReadResult> latchedActionListener
        ) {
            return null;
        }
    }
}
