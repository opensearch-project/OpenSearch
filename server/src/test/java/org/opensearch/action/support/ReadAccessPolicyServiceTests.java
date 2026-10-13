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

import static org.mockito.Mockito.mock;

public class ReadAccessPolicyServiceTests extends OpenSearchTestCase {

    public void testReturnsUnrestrictedPolicyWithoutPlugin() {
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of());

        assertSame(ReadAccessPolicy.unrestricted(), service.getReadAccessPolicy(context()));
    }

    public void testDelegatesToInstalledProvider() {
        ReadAccessPolicy expected = mock(ReadAccessPolicy.class);
        AccessPolicyProviderPlugin plugin = () -> context -> expected;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));

        assertSame(expected, service.getReadAccessPolicy(context()));
    }

    public void testRejectsMultipleProviders() {
        AccessPolicyProviderPlugin first = () -> context -> ReadAccessPolicy.unrestricted();
        AccessPolicyProviderPlugin second = () -> context -> ReadAccessPolicy.unrestricted();

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> new ReadAccessPolicyService(List.of(first, second))
        );
        assertEquals("Only one AccessPolicyProviderPlugin may be installed, found [2]", exception.getMessage());
    }

    public void testRejectsNullPolicy() {
        AccessPolicyProviderPlugin plugin = () -> context -> null;
        ReadAccessPolicyService service = new ReadAccessPolicyService(List.of(plugin));

        NullPointerException exception = expectThrows(NullPointerException.class, () -> service.getReadAccessPolicy(context()));
        assertEquals("ReadAccessPolicyProvider returned null", exception.getMessage());
    }

    private ReadAccessContext context() {
        return ReadAccessContext.of(List.of("logs"));
    }
}
