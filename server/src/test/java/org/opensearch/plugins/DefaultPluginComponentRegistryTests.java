/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

public class DefaultPluginComponentRegistryTests extends OpenSearchTestCase {

    public void testGetComponentReturnsRegisteredInstance() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        String component = "hello";
        registry.register(component);

        Optional<String> result = registry.getComponent(String.class);
        assertTrue(result.isPresent());
        assertSame(component, result.get());
    }

    public void testGetComponentReturnsEmptyForUnregisteredType() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        registry.register("a string");

        Optional<Integer> result = registry.getComponent(Integer.class);
        assertFalse(result.isPresent());
    }

    public void testGetComponentMatchesByInterface() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        CharSequence value = "implements CharSequence";
        registry.register(value);

        Optional<CharSequence> result = registry.getComponent(CharSequence.class);
        assertTrue(result.isPresent());
        assertSame(value, result.get());
    }

    public void testGetComponentReturnsFirstMatch() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        String first = "first";
        String second = "second";
        registry.register(first);
        registry.register(second);

        Optional<String> result = registry.getComponent(String.class);
        assertTrue(result.isPresent());
        assertSame(first, result.get());
    }

    public void testRegistrationOrderPreservedAcrossTypes() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();

        ServiceA a = new ServiceA();
        ServiceB b = new ServiceB();
        registry.register(a);
        registry.register(b);

        assertSame(a, registry.getComponent(ServiceA.class).orElseThrow());
        assertSame(b, registry.getComponent(ServiceB.class).orElseThrow());
    }

    public void testSubclassMatchesSuperclassLookup() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        ServiceB b = new ServiceB();
        registry.register(b);

        Optional<ServiceA> result = registry.getComponent(ServiceA.class);
        assertTrue(result.isPresent());
        assertSame(b, result.get());
    }

    public void testSealPreventsRegistration() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        registry.register("before seal");
        registry.seal();

        expectThrows(IllegalStateException.class, () -> registry.register("after seal"));
    }

    public void testSealPreventsLookup() {
        DefaultPluginComponentRegistry registry = new DefaultPluginComponentRegistry();
        registry.register("before seal");
        registry.seal();

        expectThrows(IllegalStateException.class, () -> registry.getComponent(String.class));
    }

    static class ServiceA {}

    static class ServiceB extends ServiceA {}
}
