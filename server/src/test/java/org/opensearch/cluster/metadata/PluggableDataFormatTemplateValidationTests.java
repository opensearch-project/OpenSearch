/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.containsString;

/**
 * Validates that, when a pluggable data format is the cluster-wide default
 * ({@code cluster.pluggable.dataformat.enabled}), templates carrying an explicit {@code index: true}
 * on a doc-values-backed field are rejected at template-creation time — so they cannot later fail
 * every index (including rollovers) the template creates. Templates that omit {@code index} or set
 * it to false are accepted, and a template that explicitly opts out of the pluggable data format
 * (producing vanilla indices) may still set {@code index: true}.
 */
public class PluggableDataFormatTemplateValidationTests extends OpenSearchSingleNodeTestCase {

    private static final String INDEX_TRUE = "{\"properties\":{\"n\":{\"type\":\"long\",\"index\":true}}}";
    private static final String INDEX_FALSE = "{\"properties\":{\"n\":{\"type\":\"long\",\"index\":false}}}";
    private static final String INDEX_OMITTED = "{\"properties\":{\"n\":{\"type\":\"long\"}}}";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        // Supplies a committer factory so EngineConfigFactory accepts pluggable-dataformat indices,
        // which the dummy index built during template validation requires.
        return Collections.singletonList(MockCommitterEnginePlugin.class);
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true).build();
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private MetadataIndexTemplateService templateService() {
        return getInstanceFromNode(MetadataIndexTemplateService.class);
    }

    /** Concatenates the message of a throwable and its full cause chain, for order-independent matching. */
    private static String fullMessage(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            sb.append(cur.getMessage()).append('\n');
        }
        return sb.toString();
    }

    private static ComponentTemplate componentTemplate(String mapping) throws Exception {
        return new ComponentTemplate(new Template(Settings.EMPTY, new CompressedXContent(mapping), null), 1L, new HashMap<>());
    }

    private static ComposableIndexTemplate composableTemplate(Settings settings, String mapping) throws Exception {
        return new ComposableIndexTemplate(
            Collections.singletonList("pdf-*"),
            new Template(settings, new CompressedXContent(mapping), null),
            null,
            null,
            null,
            null
        );
    }

    public void testComponentTemplateRejectsIndexTrue() throws Exception {
        MetadataIndexTemplateService service = templateService();
        Exception e = expectThrows(
            Exception.class,
            () -> service.addComponentTemplate(ClusterState.EMPTY_STATE, false, "ct", componentTemplate(INDEX_TRUE))
        );
        assertThat(fullMessage(e), containsString("cannot set [index] to true on an index using a pluggable data format"));
    }

    public void testComponentTemplateAcceptsIndexFalse() throws Exception {
        MetadataIndexTemplateService service = templateService();
        ClusterState state = service.addComponentTemplate(ClusterState.EMPTY_STATE, false, "ct", componentTemplate(INDEX_FALSE));
        assertNotNull(state.metadata().componentTemplates().get("ct"));
    }

    public void testComponentTemplateAcceptsIndexOmitted() throws Exception {
        MetadataIndexTemplateService service = templateService();
        ClusterState state = service.addComponentTemplate(ClusterState.EMPTY_STATE, false, "ct", componentTemplate(INDEX_OMITTED));
        assertNotNull(state.metadata().componentTemplates().get("ct"));
    }

    public void testComposableTemplateRejectsIndexTrue() throws Exception {
        MetadataIndexTemplateService service = templateService();
        Exception e = expectThrows(
            Exception.class,
            () -> service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "it", composableTemplate(Settings.EMPTY, INDEX_TRUE))
        );
        // The composable path wraps the mapper rejection; assert on the full cause chain.
        assertThat(fullMessage(e), containsString("cannot set [index] to true on an index using a pluggable data format"));
    }

    public void testComposableTemplateAcceptsIndexFalse() throws Exception {
        MetadataIndexTemplateService service = templateService();
        ClusterState state = service.addIndexTemplateV2(
            ClusterState.EMPTY_STATE,
            false,
            "it",
            composableTemplate(Settings.EMPTY, INDEX_FALSE)
        );
        assertNotNull(state.metadata().templatesV2().get("it"));
    }

    /**
     * A template that explicitly opts out of the pluggable data format produces vanilla indices, where
     * index: true is valid — so it must be accepted even while the cluster default is on.
     */
    public void testComposableTemplateWithExplicitOptOutAcceptsIndexTrue() throws Exception {
        MetadataIndexTemplateService service = templateService();
        Settings optOut = Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false).build();
        ClusterState state = service.addIndexTemplateV2(ClusterState.EMPTY_STATE, false, "it", composableTemplate(optOut, INDEX_TRUE));
        assertNotNull(state.metadata().templatesV2().get("it"));
    }

    /**
     * The simulate API resolves the composed template against a dummy pluggable index, so a preview of
     * a template with index: true is rejected the same way real index creation would be — the preview
     * must not report success for a template that could never create an index.
     */
    public void testSimulateResolveTemplateRejectsIndexTrue() throws Exception {
        // Stage the template directly in metadata, bypassing put-time validation, to exercise the
        // resolve/simulate path in isolation.
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put("it", composableTemplate(Settings.EMPTY, INDEX_TRUE)))
            .build();
        Exception e = expectThrows(
            Exception.class,
            () -> TransportSimulateIndexTemplateAction.resolveTemplate(
                "it",
                "pdf-1",
                state,
                xContentRegistry(),
                getInstanceFromNode(IndicesService.class),
                new AliasValidator(),
                true
            )
        );
        assertThat(fullMessage(e), containsString("cannot set [index] to true on an index using a pluggable data format"));
    }

    /** A template with index: false resolves cleanly through the simulate path. */
    public void testSimulateResolveTemplateAcceptsIndexFalse() throws Exception {
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .metadata(Metadata.builder().put("it", composableTemplate(Settings.EMPTY, INDEX_FALSE)))
            .build();
        Template resolved = TransportSimulateIndexTemplateAction.resolveTemplate(
            "it",
            "pdf-1",
            state,
            xContentRegistry(),
            getInstanceFromNode(IndicesService.class),
            new AliasValidator(),
            true
        );
        assertNotNull(resolved);
    }
}
