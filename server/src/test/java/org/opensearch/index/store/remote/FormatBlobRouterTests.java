/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link FormatBlobRouter}.
 */
public class FormatBlobRouterTests extends OpenSearchTestCase {

    private BlobStore blobStore;
    private BlobPath basePath;
    private BlobContainer baseContainer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blobStore = mock(BlobStore.class);
        basePath = new BlobPath().add("indices").add("shard0").add("segments");
        baseContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(basePath)).thenReturn(baseContainer);
    }

    public void testLuceneFormatReturnsBaseContainer() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertSame(baseContainer, router.containerFor("lucene"));
    }

    public void testNullFormatReturnsBaseContainer() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertSame(baseContainer, router.containerFor(null));
    }

    public void testMetadataFormatReturnsBaseContainer() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertSame(baseContainer, router.containerFor("metadata"));
    }

    public void testNonLuceneFormatCreatesSubPathContainer() {
        BlobContainer parquetContainer = mock(BlobContainer.class);
        BlobPath parquetPath = basePath.add("parquet");
        when(blobStore.blobContainer(parquetPath)).thenReturn(parquetContainer);

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        BlobContainer result = router.containerFor("parquet");

        assertSame(parquetContainer, result);
        verify(blobStore).blobContainer(parquetPath);
    }

    public void testSameFormatReturnsSameContainer() {
        BlobContainer parquetContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(basePath.add("parquet"))).thenReturn(parquetContainer);

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        BlobContainer first = router.containerFor("parquet");
        BlobContainer second = router.containerFor("parquet");

        assertSame(first, second);
        // blobContainer called once for basePath (constructor) + once for parquet (first access)
        verify(blobStore, times(1)).blobContainer(basePath.add("parquet"));
    }

    public void testBaseContainerAccessor() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertSame(baseContainer, router.baseContainer());
    }

    public void testRegisteredFormatsIncludesLuceneByDefault() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertTrue(router.registeredFormats().contains("lucene"));
        assertEquals(1, router.registeredFormats().size());
    }

    public void testRegisteredFormatsGrowsWithAccess() {
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(mock(BlobContainer.class));

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.containerFor("parquet");
        router.containerFor("arrow");

        assertTrue(router.registeredFormats().contains("lucene"));
        assertTrue(router.registeredFormats().contains("parquet"));
        assertTrue(router.registeredFormats().contains("arrow"));
        assertEquals(3, router.registeredFormats().size());
    }

    public void testRegisterFormatPreCreatesContainer() {
        BlobContainer parquetContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(basePath.add("parquet"))).thenReturn(parquetContainer);

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerFormat("parquet");

        assertTrue(router.registeredFormats().contains("parquet"));
        verify(blobStore).blobContainer(basePath.add("parquet"));
    }

    public void testRegisterFormatIgnoresBasePathFormats() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerFormat("lucene");
        router.registerFormat("metadata");
        router.registerFormat(null);

        // Only lucene should be registered (default), no extra containers created
        assertEquals(1, router.registeredFormats().size());
    }

    public void testFormatNameIsLowercasedForPath() {
        BlobContainer container = mock(BlobContainer.class);
        when(blobStore.blobContainer(basePath.add("parquet"))).thenReturn(container);

        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.containerFor("Parquet");

        // Should create path with lowercase "parquet"
        verify(blobStore).blobContainer(basePath.add("parquet"));
    }

    // ═══════════════════════════════════════════════════════════════
    // Blob Format Cache Tests
    // ═══════════════════════════════════════════════════════════════

    public void testResolveFormat_DefaultsToLucene() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        assertEquals("lucene", router.resolveFormat("_0.cfs__UUID"));
    }

    public void testRegisterBlobFormat_ThenResolve() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("_0.pqt__UUID", "parquet");
        assertEquals("parquet", router.resolveFormat("_0.pqt__UUID"));
    }

    public void testRegisterBlobFormat_NullKeyIgnored() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat(null, "parquet");
        // Should not throw, cache unchanged
        assertEquals("lucene", router.resolveFormat("anything"));
    }

    public void testRegisterBlobFormat_NullFormatIgnored() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("_0.pqt__UUID", null);
        assertEquals("lucene", router.resolveFormat("_0.pqt__UUID"));
    }

    public void testUnregisterBlobFormat() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("_0.pqt__UUID", "parquet");
        assertEquals("parquet", router.resolveFormat("_0.pqt__UUID"));

        router.unregisterBlobFormat("_0.pqt__UUID");
        assertEquals("lucene", router.resolveFormat("_0.pqt__UUID"));
    }

    public void testUnregisterBlobFormat_NullKeyIgnored() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("_0.pqt__UUID", "parquet");
        router.unregisterBlobFormat(null);
        // Cache unchanged
        assertEquals("parquet", router.resolveFormat("_0.pqt__UUID"));
    }

    public void testReplaceBlobFormatCache() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("old_key", "parquet");

        router.replaceBlobFormatCache(java.util.Map.of("new_key", "arrow"));

        assertEquals("lucene", router.resolveFormat("old_key"));
        assertEquals("arrow", router.resolveFormat("new_key"));
    }

    public void testClearBlobFormatCache() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        router.registerBlobFormat("_0.pqt__UUID", "parquet");
        assertEquals("parquet", router.resolveFormat("_0.pqt__UUID"));

        router.clearBlobFormatCache();
        assertEquals("lucene", router.resolveFormat("_0.pqt__UUID"));
    }

    public void testReplaceBlobFormatCache_ImmutableSnapshot() {
        FormatBlobRouter router = new FormatBlobRouter(blobStore, basePath);
        java.util.Map<String, String> mutable = new java.util.HashMap<>();
        mutable.put("key1", "parquet");
        router.replaceBlobFormatCache(mutable);

        // Mutating the original map should not affect the cache
        mutable.put("key2", "arrow");
        assertEquals("lucene", router.resolveFormat("key2"));
    }
}
