/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.blobstore;

import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BlobContainerTests extends OpenSearchTestCase {

    public void testWriteBlobWithMetadataDefaultWritesWithoutMetadata() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class, CALLS_REAL_METHODS);
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);

        blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, null);
        blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, Collections.emptyMap());
        blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, null, null);
        blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, Collections.emptyMap(), null);

        verify(blobContainer, times(4)).writeBlob(anyString(), any(InputStream.class), anyLong(), anyBoolean());
    }

    public void testWriteBlobWithMetadataDefaultsRejectUnsupportedOptionsBeforeWriting() throws IOException {
        BlobContainer blobContainer = mock(BlobContainer.class, CALLS_REAL_METHODS);
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        Map<String, String> metadata = Map.of("key", "value");
        CryptoMetadata cryptoMetadata = new CryptoMetadata("provider", "type", Settings.EMPTY);

        IOException metadataException = expectThrows(
            IOException.class,
            () -> blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, metadata)
        );
        assertThat(metadataException.getMessage(), containsString("metadata"));

        IOException metadataAndCryptoException = expectThrows(
            IOException.class,
            () -> blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, metadata, null)
        );
        assertThat(metadataAndCryptoException.getMessage(), containsString("metadata"));

        for (Map<String, String> userMetadata : Arrays.asList(null, Collections.<String, String>emptyMap(), metadata)) {
            IOException cryptoException = expectThrows(
                IOException.class,
                () -> blobContainer.writeBlobWithMetadata("blob", inputStream, 0, true, userMetadata, cryptoMetadata)
            );
            assertThat(cryptoException.getMessage(), containsString("CryptoMetadata"));
        }

        verify(blobContainer, never()).writeBlob(anyString(), any(InputStream.class), anyLong(), anyBoolean());
    }
}
