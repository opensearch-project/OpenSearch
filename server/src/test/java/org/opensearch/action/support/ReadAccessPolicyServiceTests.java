/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class ReadAccessPolicyServiceTests extends OpenSearchTestCase {

    public void testReturnsUnrestrictedPolicyWithoutPlugin() {
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of());
        service.initialize();

        assertSame(DocumentLevelReadAccessPolicy.unrestricted(), service.getDocumentLevelReadAccessPolicy(context()));
    }

    public void testDelegatesToInstalledProvider() {
        DocumentLevelReadAccessPolicy expected = mock(DocumentLevelReadAccessPolicy.class);
        AccessPolicyProviderPlugin plugin = () -> context -> expected;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));
        service.initialize();

        assertSame(expected, service.getDocumentLevelReadAccessPolicy(context()));
    }

    public void testProviderIsResolvedAfterComponentCreation() {
        DocumentLevelReadAccessPolicy expected = mock(DocumentLevelReadAccessPolicy.class);
        AtomicReference<ReadAccessPolicyProvider> availableProvider = new AtomicReference<>();
        AccessPolicyProviderPlugin plugin = availableProvider::get;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));

        assertEquals(
            "ReadAccessPolicyService is not initialized",
            expectThrows(IllegalStateException.class, () -> service.getDocumentLevelReadAccessPolicy(context())).getMessage()
        );
        availableProvider.set(context -> expected);
        service.initialize();
        assertSame(expected, service.getDocumentLevelReadAccessPolicy(context()));
    }

    public void testRejectsMultipleProviders() {
        AccessPolicyProviderPlugin first = () -> context -> DocumentLevelReadAccessPolicy.unrestricted();
        AccessPolicyProviderPlugin second = () -> context -> DocumentLevelReadAccessPolicy.unrestricted();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> new ReadAccessPolicyService(List.of(first, second))
        );
        assertEquals("Only one AccessPolicyProviderPlugin may be installed, found [2]", exception.getMessage());
    }

    public void testRejectsNullPolicy() {
        AccessPolicyProviderPlugin plugin = () -> context -> null;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));
        service.initialize();

        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> service.getDocumentLevelReadAccessPolicy(context())
        );
        assertEquals("ReadAccessPolicyProvider returned null", exception.getMessage());
    }

    public void testRejectsNullProvider() {
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(() -> null));

        NullPointerException exception = expectThrows(NullPointerException.class, service::initialize);
        assertEquals("AccessPolicyProviderPlugin returned null", exception.getMessage());
    }

    private ReadAccessContext context() {
        return ReadAccessContext.of(List.of("logs"));
    }
}
