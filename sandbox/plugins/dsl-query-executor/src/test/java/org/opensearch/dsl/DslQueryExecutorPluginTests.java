/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsException;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.dsl.action.DslExecuteAction;
import org.opensearch.dsl.action.SearchActionFilter;
import org.opensearch.dsl.action.TransportDslExecuteAction;
import org.opensearch.dsl.settings.DslGateInputs;
import org.opensearch.dsl.settings.DslQuerySettings;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DslQueryExecutorPluginTests extends OpenSearchTestCase {

    /**
     * The width setting's upper bound, mirroring {@code SubPlanParallelism.MAX_K_SETTING}
     * (package-private in another package). Pinned to it by
     * {@code SubPlanParallelismTests#testTheSettingsBoundMatchesTheHardCeiling}.
     */
    private static final int CEILING = 5;

    private DslQueryExecutorPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new DslQueryExecutorPlugin();
    }

    public void testGetActionFiltersEmptyBeforeCreateComponents() {
        List<ActionFilter> filters = plugin.getActionFilters();

        assertTrue(filters.isEmpty());
    }

    public void testGetActionFiltersAfterCreateComponents() {
        createComponents();

        List<ActionFilter> filters = plugin.getActionFilters();
        assertEquals(1, filters.size());
        assertTrue(filters.get(0) instanceof SearchActionFilter);
    }

    /**
     * A setting missing from {@code getSettings()} is invisible to {@code _cluster/settings} (a PUT of
     * it is a 400) and not resolvable by key from another plugin's classloader. Spelled out as literal keys
     * rather than derived from {@code DslQuerySettings.all()}: the keys are the operator-facing contract, so
     * a rename has to be visible here.
     */
    public void testGetSettingsRegistersTheFanOutWidthAndNothingElse() {
        Set<String> keys = plugin.getSettings().stream().map(Setting::getKey).collect(Collectors.toSet());

        // Exactly one key: the fan-out is configured by a width and by nothing else. assertEquals on the
        // whole set, not a contains check, so an added registration fails here.
        assertEquals(Set.of("dsl.query.max_parallel_sub_plans"), keys);
    }

    /**
     * Identity, not key equality. {@code Setting.equals} compares keys only, but
     * {@code ClusterSettings.addSettingsUpdateConsumer} rejects any descriptor that is not the very
     * instance the node registered (AbstractScopedSettings.java:256). So if {@code getSettings()} handed
     * out key-equal copies instead of the constants the holder subscribes to, the node would register the
     * copies and {@code DslQuerySettings}' constructor would throw {@code SettingsException} — the plugin
     * would fail to load. The key-set assertion above cannot see that; this can.
     */
    public void testGetSettingsRegistersTheSameDescriptorsTheHoldersSubscribeTo() {
        List<Setting<?>> registered = plugin.getSettings();

        assertEquals("getSettings() must register every setting all() declares", DslQuerySettings.all().size(), registered.size());
        for (Setting<?> declared : DslQuerySettings.all()) {
            assertSameDescriptorRegistered(registered, declared);
        }
    }

    private static void assertSameDescriptorRegistered(List<Setting<?>> registered, Setting<?> declared) {
        Setting<?> match = registered.stream().filter(s -> s.getKey().equals(declared.getKey())).findFirst().orElse(null);
        assertNotNull("getSettings() does not register " + declared.getKey(), match);
        assertSame("getSettings() must register the descriptor instance itself, not a key-equal copy", declared, match);
    }

    /**
     * Both holders must be returned so Guice can inject them into the transport action: without the
     * settings holder the execution path has no route to {@code dsl.query.max_parallel_sub_plans}, and
     * without the gate-input reader the cross-plugin read path has no production construction site at
     * all — it would be reachable only from hand-built test registries.
     */
    public void testCreateComponentsReturnsDslQuerySettingsAndGateInputs() {
        Set<Class<?>> types = createComponents().stream().map(Object::getClass).collect(Collectors.toSet());

        assertEquals(
            "expected exactly the two settings components, got " + types,
            Set.of(DslQuerySettings.class, DslGateInputs.class),
            types
        );
    }

    private Collection<Object> createComponents() {
        // createComponents now dereferences clusterService to build the settings holder, so the
        // previous null here would NPE. The constructor is deliberately not null-tolerant.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(Settings.EMPTY, Set.copyOf(plugin.getSettings())));
        return plugin.createComponents(mock(NodeClient.class), clusterService, null, null, null, null, null, null, null, null, null);
    }

    /**
     * The registration mechanism itself, exercised through the class a node builds its registry with.
     */
    public void testSettingsModuleMakesTheWidthKeyResolvableByString() {
        ClusterSettings registered = nodeRegistry(plugin.getSettings());

        assertSame(DslQuerySettings.MAX_PARALLEL_SUB_PLANS, registered.get("dsl.query.max_parallel_sub_plans"));
        assertNull("no execution shape may be resolvable as a setting", registered.get("dsl.query.fanout_launch"));
    }

    /**
     * The cap at the layer a {@code _cluster/settings} PUT actually hits: the transport action validates
     * the submitted settings against the node's registry before applying them, and that validation is
     * what turns a {@code 3} into a 400. A cap enforced only by a downstream {@code min} passes
     * {@code DslQuerySettingsTests} and fails here.
     */
    public void testSettingsModuleValidationRejectsAboveTheCeiling() {
        ClusterSettings registered = nodeRegistry(plugin.getSettings());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> registered.validate(Settings.builder().put("dsl.query.max_parallel_sub_plans", CEILING + 1).build(), true)
        );
        assertTrue("expected an upper-bound message, got: " + e.getMessage(), e.getMessage().contains("must be <= " + CEILING));

        // The accepted leg, so the test cannot pass against a registry that rejects everything.
        registered.validate(Settings.builder().put("dsl.query.max_parallel_sub_plans", CEILING).build(), true);
    }

    /**
     * The execution shape must be <b>unsettable</b> at the layer a real {@code _cluster/settings} PUT is
     * validated against. An unregistered key is rejected before any value parsing, so this is the check that
     * would fail if someone re-registered the shape as a setting — which is the whole point of it being a
     * constant. {@code SettingsException} rather than {@code IllegalArgumentException}: an unknown key never
     * reaches a parser.
     */
    public void testSettingsModuleRejectsTheLaunchShapeAsAnUnknownSetting() {
        ClusterSettings registered = nodeRegistry(plugin.getSettings());

        SettingsException e = expectThrows(
            SettingsException.class,
            () -> registered.validate(Settings.builder().put("dsl.query.fanout_launch", "flat").build(), true)
        );
        assertTrue("expected an unknown-setting message, got: " + e.getMessage(), e.getMessage().contains("unknown setting"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    /**
     * The other side of the same mechanism, and the reason the sweep harness is blocked until this
     * plugin registers its settings: without {@code getSettings()} the key is not merely inert, it is
     * <b>rejected</b> as unknown — a 400 over REST, a startup failure in {@code opensearch.yml}.
     */
    public void testKeysAreUnknownToANodeThatDoesNotRegisterThem() {
        ClusterSettings withoutDslSettings = nodeRegistry(List.of());

        // SettingsException, not IllegalArgumentException: an unregistered key is rejected before any
        // value parsing happens. It carries RestStatus.BAD_REQUEST all the same, so the operator-visible
        SettingsException e = expectThrows(
            SettingsException.class,
            () -> withoutDslSettings.validate(Settings.builder().put("dsl.query.max_parallel_sub_plans", 2).build(), true)
        );
        assertTrue("expected an unknown-setting message, got: " + e.getMessage(), e.getMessage().contains("unknown setting"));
        assertEquals(RestStatus.BAD_REQUEST, e.status());
    }

    /** The registry a node ends up with, assembled the way {@code Node} assembles it. */
    private static ClusterSettings nodeRegistry(List<Setting<?>> pluginSettings) {
        return new SettingsModule(Settings.EMPTY, pluginSettings, List.of(), Set.of()).getClusterSettings();
    }

    public void testRegistersTransportAction() {
        var actions = plugin.getActions();

        assertEquals(1, actions.size());
        ActionPlugin.ActionHandler<?, ?> handler = actions.get(0);
        assertEquals(DslExecuteAction.INSTANCE, handler.getAction());
        assertEquals(TransportDslExecuteAction.class, handler.getTransportAction());
    }
}
