/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.rule.action.GetRuleAction;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.rest.RestCreateRuleAction;
import org.opensearch.rule.rest.RestDeleteRuleAction;
import org.opensearch.rule.rest.RestGetRuleAction;
import org.opensearch.rule.rest.RestUpdateRuleAction;
import org.opensearch.rule.spi.RuleFrameworkExtension;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RuleFrameworkPluginTests extends OpenSearchTestCase {
    RuleFrameworkPlugin plugin = new RuleFrameworkPlugin();

    public void testGetActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> handlers = plugin.getActions();
        assertEquals(4, handlers.size());
        assertEquals(GetRuleAction.INSTANCE.name(), handlers.get(0).getAction().name());
    }

    public void testGetRestHandlers() {
        Settings settings = Settings.EMPTY;
        List<RestHandler> handlers = plugin.getRestHandlers(
            settings,
            mock(org.opensearch.rest.RestController.class),
            null,
            null,
            null,
            mock(IndexNameExpressionResolver.class),
            () -> mock(DiscoveryNodes.class)
        );

        assertTrue(handlers.get(0) instanceof RestGetRuleAction);
        assertTrue(handlers.get(1) instanceof RestDeleteRuleAction);
        assertTrue(handlers.get(2) instanceof RestCreateRuleAction);
        assertTrue(handlers.get(3) instanceof RestUpdateRuleAction);
    }

    public void testReadersResolveFeatureAfterComponentsAreCreated() throws Exception {
        AtomicReference<FeatureType> feature = new AtomicReference<>();
        AtomicInteger lookups = new AtomicInteger();
        RuleFrameworkPlugin plugin = plugin(extension("test_feature", () -> {
            lookups.incrementAndGet();
            return feature.get();
        }));
        NamedWriteableRegistry readers = new NamedWriteableRegistry(plugin.getNamedWriteables());
        assertEquals(0, lookups.get());

        feature.set(featureType("test_feature"));
        plugin.getActions();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Bytes written by the previous implementation contain only the feature name.
            out.writeString("test_feature");
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), readers)) {
                assertSame(feature.get(), FeatureType.from(in));
                assertEquals(0, in.available());
            }
        }
    }

    public void testDuplicateFeatureReaderNamesAreRejected() {
        RuleFrameworkPlugin plugin = plugin(
            extension("test_feature", () -> featureType("test_feature")),
            extension("test_feature", () -> featureType("test_feature"))
        );
        assertThrows(IllegalArgumentException.class, () -> new NamedWriteableRegistry(plugin.getNamedWriteables()));
    }

    public void testDeclaredNameMustMatchInitializedFeature() {
        RuleFrameworkPlugin plugin = plugin(extension("test_feature", () -> featureType("different_feature")));
        assertThrows(IllegalStateException.class, plugin::getActions);
    }

    private static RuleFrameworkPlugin plugin(RuleFrameworkExtension... extensions) {
        RuleFrameworkPlugin plugin = new RuleFrameworkPlugin();
        plugin.loadExtensions(new ExtensiblePlugin.ExtensionLoader() {
            @Override
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                if (extensionPointType == RuleFrameworkExtension.class) {
                    return java.util.Arrays.stream(extensions).map(extensionPointType::cast).toList();
                }
                return List.of();
            }
        });
        return plugin;
    }

    private static RuleFrameworkExtension extension(String name, Supplier<FeatureType> feature) {
        RuleFrameworkExtension extension = mock(RuleFrameworkExtension.class);
        when(extension.getFeatureTypeName()).thenReturn(name);
        when(extension.getFeatureTypeSupplier()).thenReturn(feature);
        when(extension.getRulePersistenceServiceSupplier()).thenReturn(() -> mock(RulePersistenceService.class));
        when(extension.getRuleRoutingServiceSupplier()).thenReturn(() -> mock(RuleRoutingService.class));
        return extension;
    }

    private static FeatureType featureType(String name) {
        return new FeatureType() {
            @Override
            public String getName() {
                return name;
            }

            @Override
            public Map<Attribute, Integer> getOrderedAttributes() {
                return Map.of();
            }
        };
    }
}
