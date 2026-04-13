/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories;

import org.opensearch.plugins.NativeRemoteObjectStoreProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link NativeRemoteObjectStoreService}.
 */
public class NativeRemoteObjectStoreServiceTests extends OpenSearchTestCase {

    // -----------------------------------------------------------------------
    // Mock provider — tracks create/destroy calls
    // -----------------------------------------------------------------------

    private static class MockProvider implements NativeRemoteObjectStoreProvider {
        private final String type;
        private final AtomicLong nextPtr = new AtomicLong(100);
        private final AtomicInteger createCount = new AtomicInteger();
        private final AtomicInteger destroyCount = new AtomicInteger();

        MockProvider(final String type) {
            this.type = type;
        }

        @Override
        public String repositoryType() {
            return type;
        }

        @Override
        public long createNativeStore(final String configJson) {
            createCount.incrementAndGet();
            return nextPtr.getAndIncrement();
        }

        @Override
        public void destroyNativeStore(final long ptr) {
            destroyCount.incrementAndGet();
        }
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    public void testEmptyProvidersList() {
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(Collections.emptyList())) {
            assertEquals(NativeRemoteObjectStoreService.STORE_NOT_FOUND, service.getStorePtr("any"));
            assertTrue(service.getAllStores().isEmpty());
            assertFalse(service.hasProvider("s3"));
        }
    }

    public void testNullProvidersListThrows() {
        expectThrows(NullPointerException.class, () -> new NativeRemoteObjectStoreService(null));
    }

    public void testDuplicateProviderTypeThrows() {
        final MockProvider p1 = new MockProvider("s3");
        final MockProvider p2 = new MockProvider("s3");
        expectThrows(IllegalArgumentException.class, () -> new NativeRemoteObjectStoreService(List.of(p1, p2)));
    }

    public void testMultipleProviderTypes() {
        final MockProvider s3 = new MockProvider("s3");
        final MockProvider gcs = new MockProvider("gcs");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(s3, gcs))) {
            assertTrue(service.hasProvider("s3"));
            assertTrue(service.hasProvider("gcs"));
            assertFalse(service.hasProvider("azure"));
        }
    }

    // -----------------------------------------------------------------------
    // ensureStore
    // -----------------------------------------------------------------------

    public void testEnsureStoreCreatesAndCaches() {
        final MockProvider provider = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider))) {
            final long ptr1 = service.ensureStore("my-repo", "s3", "{}");
            assertTrue(ptr1 > 0);
            assertEquals(1, provider.createCount.get());

            // Second call returns cached pointer — no new creation
            final long ptr2 = service.ensureStore("my-repo", "s3", "{}");
            assertEquals(ptr1, ptr2);
            assertEquals(1, provider.createCount.get());
        }
    }

    public void testEnsureStoreDifferentRepos() {
        final MockProvider provider = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider))) {
            final long ptr1 = service.ensureStore("repo-a", "s3", "{}");
            final long ptr2 = service.ensureStore("repo-b", "s3", "{}");
            assertNotEquals(ptr1, ptr2);
            assertEquals(2, provider.createCount.get());
        }
    }

    public void testEnsureStoreUnknownTypeThrows() {
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(Collections.emptyList())) {
            expectThrows(RepositoryException.class, () -> service.ensureStore("repo", "s3", "{}"));
        }
    }

    public void testEnsureStoreCreationFailureThrows() {
        final NativeRemoteObjectStoreProvider failProvider = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "bad";
            }

            @Override
            public long createNativeStore(final String configJson) {
                return -1; // simulate failure
            }

            @Override
            public void destroyNativeStore(final long ptr) {}
        };
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(failProvider))) {
            expectThrows(RepositoryException.class, () -> service.ensureStore("repo", "bad", "{}"));
            // Not cached after failure
            assertEquals(NativeRemoteObjectStoreService.STORE_NOT_FOUND, service.getStorePtr("repo"));
        }
    }

    // -----------------------------------------------------------------------
    // getStorePtr / getAllStores
    // -----------------------------------------------------------------------

    public void testGetStorePtrNotFound() {
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(Collections.emptyList())) {
            assertEquals(NativeRemoteObjectStoreService.STORE_NOT_FOUND, service.getStorePtr("nonexistent"));
        }
    }

    public void testGetAllStores() {
        final MockProvider s3 = new MockProvider("s3");
        final MockProvider gcs = new MockProvider("gcs");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(s3, gcs))) {
            service.ensureStore("s3-repo", "s3", "{}");
            service.ensureStore("gcs-repo", "gcs", "{}");

            final Map<String, Long> all = service.getAllStores();
            assertEquals(2, all.size());
            assertTrue(all.containsKey("s3-repo"));
            assertTrue(all.containsKey("gcs-repo"));
            assertTrue(all.get("s3-repo") > 0);
            assertTrue(all.get("gcs-repo") > 0);
        }
    }

    public void testGetAllStoresIsUnmodifiable() {
        final MockProvider provider = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider))) {
            service.ensureStore("repo", "s3", "{}");
            final Map<String, Long> all = service.getAllStores();
            expectThrows(UnsupportedOperationException.class, () -> all.put("new", 999L));
        }
    }

    // -----------------------------------------------------------------------
    // close
    // -----------------------------------------------------------------------

    public void testCloseDestroysAllStores() {
        final MockProvider s3 = new MockProvider("s3");
        final MockProvider gcs = new MockProvider("gcs");
        final NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(s3, gcs));
        service.ensureStore("s3-repo", "s3", "{}");
        service.ensureStore("gcs-repo", "gcs", "{}");

        service.close();

        assertEquals(1, s3.destroyCount.get());
        assertEquals(1, gcs.destroyCount.get());
        assertTrue(service.getAllStores().isEmpty());
    }

    public void testCloseIsIdempotent() {
        final MockProvider provider = new MockProvider("s3");
        final NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider));
        service.ensureStore("repo", "s3", "{}");

        service.close();
        service.close(); // second close — no double-destroy

        assertEquals(1, provider.destroyCount.get());
    }

    public void testCloseHandlesDestroyFailure() {
        final NativeRemoteObjectStoreProvider failDestroy = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "fail";
            }

            @Override
            public long createNativeStore(final String configJson) {
                return 42;
            }

            @Override
            public void destroyNativeStore(final long ptr) {
                throw new RuntimeException("destroy failed");
            }
        };
        final NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(failDestroy));
        service.ensureStore("repo", "fail", "{}");

        // close should not throw even if destroy fails
        service.close();
        assertTrue(service.getAllStores().isEmpty());
    }

    // -----------------------------------------------------------------------
    // Credential provider passthrough
    // -----------------------------------------------------------------------

    public void testEnsureStoreWithCredentialProvider() {
        final AtomicLong receivedCredPtr = new AtomicLong();
        final NativeRemoteObjectStoreProvider credProvider = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "s3";
            }

            @Override
            public long createNativeStore(final String configJson) {
                return createNativeStore(configJson, 0L);
            }

            @Override
            public long createNativeStore(final String configJson, final long credProviderPtr) {
                receivedCredPtr.set(credProviderPtr);
                return 99;
            }

            @Override
            public void destroyNativeStore(final long ptr) {}
        };
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(credProvider))) {
            service.ensureStore("repo", "s3", "{}", 12345L);
            assertEquals(12345L, receivedCredPtr.get());
        }
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    public void testConcurrentEnsureStoreSameRepo() throws Exception {
        final MockProvider provider = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider))) {
            final int threadCount = 10;
            final CyclicBarrier barrier = new CyclicBarrier(threadCount);
            final CountDownLatch done = new CountDownLatch(threadCount);
            final long[] results = new long[threadCount];

            for (int i = 0; i < threadCount; i++) {
                final int idx = i;
                new Thread(() -> {
                    try {
                        barrier.await();
                        results[idx] = service.ensureStore("repo", "s3", "{}");
                    } catch (final Exception e) {
                        results[idx] = -999;
                    } finally {
                        done.countDown();
                    }
                }).start();
            }

            done.await();

            // All threads should get the same pointer
            final long expected = results[0];
            for (int i = 1; i < threadCount; i++) {
                assertEquals("Thread " + i + " got different pointer", expected, results[i]);
            }
            // Only one creation should have happened
            assertEquals(1, provider.createCount.get());
        }
    }

    // -----------------------------------------------------------------------
    // Additional failure / edge case coverage
    // -----------------------------------------------------------------------

    public void testEnsureStoreCreateThrowsRuntimeException() {
        final NativeRemoteObjectStoreProvider throwProvider = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "boom";
            }

            @Override
            public long createNativeStore(final String configJson) {
                throw new RuntimeException("FFM call failed");
            }

            @Override
            public void destroyNativeStore(final long ptr) {}
        };
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(throwProvider))) {
            RuntimeException ex = expectThrows(RuntimeException.class, () -> service.ensureStore("repo", "boom", "{}"));
            assertTrue(ex.getMessage().contains("FFM call failed"));
            // Not cached after exception
            assertEquals(NativeRemoteObjectStoreService.STORE_NOT_FOUND, service.getStorePtr("repo"));
        }
    }

    public void testEnsureStoreReturnsZeroPtrThrows() {
        final NativeRemoteObjectStoreProvider zeroProvider = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "zero";
            }

            @Override
            public long createNativeStore(final String configJson) {
                return 0; // zero is also invalid
            }

            @Override
            public void destroyNativeStore(final long ptr) {}
        };
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(zeroProvider))) {
            expectThrows(RepositoryException.class, () -> service.ensureStore("repo", "zero", "{}"));
        }
    }

    public void testGetStorePtrAfterEnsure() {
        final MockProvider provider = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider))) {
            final long ptr = service.ensureStore("repo", "s3", "{}");
            assertEquals(ptr, service.getStorePtr("repo"));
        }
    }

    public void testEnsureStoreAfterClose() {
        final MockProvider provider = new MockProvider("s3");
        final NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(provider));
        service.ensureStore("repo-a", "s3", "{}");
        service.close();

        // After close, stores map is cleared — ensureStore creates a new one
        final long ptr = service.ensureStore("repo-b", "s3", "{}");
        assertTrue(ptr > 0);
        assertEquals(2, provider.createCount.get());
    }

    public void testDefaultCredentialProviderDelegation() {
        // Verify the default method on the interface delegates to 1-arg
        final AtomicInteger oneArgCalled = new AtomicInteger();
        final NativeRemoteObjectStoreProvider defaultImpl = new NativeRemoteObjectStoreProvider() {
            @Override
            public String repositoryType() {
                return "test";
            }

            @Override
            public long createNativeStore(final String configJson) {
                oneArgCalled.incrementAndGet();
                return 77;
            }

            @Override
            public void destroyNativeStore(final long ptr) {}
        };
        // Call the 2-arg default method — should delegate to 1-arg
        final long ptr = defaultImpl.createNativeStore("{}", 999L);
        assertEquals(77, ptr);
        assertEquals(1, oneArgCalled.get());
    }

    public void testEnsureStoreWithProviderExistingForDifferentType() {
        final MockProvider s3 = new MockProvider("s3");
        try (NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(s3))) {
            // Provider exists for s3 but not gcs
            assertTrue(service.hasProvider("s3"));
            expectThrows(RepositoryException.class, () -> service.ensureStore("gcs-repo", "gcs", "{}"));
        }
    }

    public void testCloseWithMultipleStoresSomeFailDestroy() {
        final AtomicInteger destroyCount = new AtomicInteger();
        final NativeRemoteObjectStoreProvider mixedProvider = new NativeRemoteObjectStoreProvider() {
            private final AtomicLong nextPtr = new AtomicLong(1);

            @Override
            public String repositoryType() {
                return "mixed";
            }

            @Override
            public long createNativeStore(final String configJson) {
                return nextPtr.getAndIncrement();
            }

            @Override
            public void destroyNativeStore(final long ptr) {
                destroyCount.incrementAndGet();
                if (ptr == 1) {
                    throw new RuntimeException("destroy ptr=1 failed");
                }
                // ptr=2 succeeds
            }
        };
        final NativeRemoteObjectStoreService service = new NativeRemoteObjectStoreService(List.of(mixedProvider));
        service.ensureStore("repo-1", "mixed", "{}");
        service.ensureStore("repo-2", "mixed", "{}");

        // close should attempt both destroys even if first fails
        service.close();
        assertEquals(2, destroyCount.get());
        assertTrue(service.getAllStores().isEmpty());
    }
}
