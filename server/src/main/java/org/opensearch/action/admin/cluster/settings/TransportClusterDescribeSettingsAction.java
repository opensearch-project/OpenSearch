/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingValidation;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.function.Function;

/** Resolves definitions using the setting registry. The registry is identical on every node of the same version and
 * never depends on cluster state, so the request is answered locally by whichever node receives it: no cluster-manager
 * round-trip, no term check, and the response never crosses the wire. For the same reason, cluster blocks are
 * deliberately not consulted: the answer is valid while the cluster state is still recovering or no cluster-manager is
 * elected, which is when an operator most needs it. Authorization is unaffected; it is applied by {@code ActionFilter}s
 * before {@link #doExecute} runs, which is why the transport action exists at all.
 * @opensearch.internal
 */
public class TransportClusterDescribeSettingsAction extends HandledTransportAction<
    ClusterDescribeSettingsRequest,
    ClusterDescribeSettingsResponse> {
    private final ClusterService clusterService;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportClusterDescribeSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters,
        SettingsFilter settingsFilter
    ) {
        super(ClusterDescribeSettingsAction.NAME, transportService, actionFilters, ClusterDescribeSettingsRequest::new);
        this.clusterService = clusterService;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected void doExecute(Task task, ClusterDescribeSettingsRequest request, ActionListener<ClusterDescribeSettingsResponse> listener) {
        listener.onResponse(describe(request, clusterService.getClusterSettings(), settingsFilter));
    }

    static ClusterDescribeSettingsResponse describe(
        ClusterDescribeSettingsRequest request,
        ClusterSettings registry,
        SettingsFilter filter
    ) {
        // Filter names using placeholders; the configured value is never read or serialized.
        Settings.Builder names = Settings.builder();
        for (String name : request.names())
            names.put(name, "");
        var result = new HashMap<String, SettingMetadata>();
        for (String name : filter.filter(names.build()).keySet()) {
            Setting<?> setting = registry.get(name);
            // Omission deliberately makes an unknown name indistinguishable from a filtered one.
            if (setting != null && setting.isFiltered() == false) {
                result.put(name, metadataOf(setting));
            }
        }
        return new ClusterDescribeSettingsResponse(result);
    }

    /**
     * Describes a single registered setting. The declared default is resolved against {@link Settings#EMPTY} so that
     * settings whose default depends on another setting report their declared default rather than a value derived
     * from this cluster's configuration.
     */
    private static SettingMetadata metadataOf(Setting<?> setting) {
        return baseMetadataOf(setting).withDescription(setting.getDescription());
    }

    /** Dynamic flag, declared default and validation; everything except the description, which applies to every kind of setting. */
    private static SettingMetadata baseMetadataOf(Setting<?> setting) {
        boolean dynamic = setting.isDynamic();
        // SecureSetting overrides getDefaultRaw to throw UnsupportedOperationException; never call it.
        if (setting instanceof SecureSetting) {
            return SettingMetadata.withoutDefault(dynamic);
        }
        String defaultValue = setting.getDefaultRaw(Settings.EMPTY);
        // The Setting constructor asserts a non-null default for every non-secure setting, so only emptiness is
        // checked here. Group settings declare an empty default; an empty string carries no information for any setting.
        SettingMetadata metadata = defaultValue.isEmpty()
            ? SettingMetadata.withoutDefault(dynamic)
            : SettingMetadata.withDefault(dynamic, defaultValue);
        return metadata.withValidation(validationOf(setting));
    }

    /**
     * Derives the declared constraints of a setting from its parser, or {@code null} when the parser is an opaque function
     * whose value type cannot be recognised. The unbounded integer and long factories delegate to the ranged overload with
     * {@code MIN_VALUE}/{@code MAX_VALUE} as sentinels; those admit every value and are suppressed rather than reported as
     * limits. {@code Float.MIN_VALUE} and {@code Double.MIN_VALUE} are positive, so a float or double parser carrying them
     * rejects zero and negatives and the minimum is reported; only their {@code MAX_VALUE} is suppressed. The setting always
     * reports its type.
     */
    @Nullable
    static SettingValidation validationOf(Setting<?> setting) {
        Function<String, ?> parser = setting.getParser();
        if (parser instanceof Setting.BooleanParser) {
            return SettingValidation.ofType("boolean");
        }
        if (parser instanceof Setting.IntegerParser p) {
            return SettingValidation.of(
                "integer",
                p.getMin() == Integer.MIN_VALUE ? null : Integer.toString(p.getMin()),
                p.getMax() == Integer.MAX_VALUE ? null : Integer.toString(p.getMax())
            );
        }
        if (parser instanceof Setting.LongParser p) {
            return SettingValidation.of(
                "long",
                p.getMin() == Long.MIN_VALUE ? null : Long.toString(p.getMin()),
                p.getMax() == Long.MAX_VALUE ? null : Long.toString(p.getMax())
            );
        }
        if (parser instanceof Setting.FloatParser p) {
            // Float.MIN_VALUE is the smallest positive float, not the most negative, so a parser carrying it as its minimum really
            // does reject zero and negatives and the minimum is reported. Only Float.MAX_VALUE is a genuine "no upper bound".
            return SettingValidation.of(
                "float",
                Float.toString(p.getMin()),
                p.getMax() == Float.MAX_VALUE ? null : Float.toString(p.getMax())
            );
        }
        if (parser instanceof Setting.DoubleParser p) {
            // Same as float: Double.MIN_VALUE is positive and therefore a real lower bound.
            return SettingValidation.of(
                "double",
                Double.toString(p.getMin()),
                p.getMax() == Double.MAX_VALUE ? null : Double.toString(p.getMax())
            );
        }
        if (parser instanceof Setting.ByteSizeValueParser p) {
            // ByteSizeValueParser(key) declares -1 and Long.MAX_VALUE bytes when no bounds are given.
            return SettingValidation.of(
                "byte_size",
                p.getMin().getBytes() < 0 ? null : p.getMin().getStringRep(),
                p.getMax().getBytes() == Long.MAX_VALUE ? null : p.getMax().getStringRep()
            );
        }
        if (parser instanceof Setting.MemorySizeValueParser) {
            // Accepts an absolute byte size or a percentage of heap (or native memory). The percentage form is hard-bounded
            // to [0, 100] by MemorySizeValue; absolute sizes carry no declared bound. The bounds reported here describe the
            // percentage form.
            return SettingValidation.of("memory_size", "0%", "100%");
        }
        if (parser instanceof Setting.MinTimeValueParser p) {
            return SettingValidation.of("time", p.getMin().getStringRep(), null);
        }
        if (parser instanceof Setting.MinMaxTimeValueParser p) {
            return SettingValidation.of("time", p.getMin().getStringRep(), p.getMax().getStringRep());
        }
        // Everything else (strings, enums, lists, plain timeSetting, secure settings) is an opaque function: neither the
        // value type nor any bound can be read from it.
        return null;
    }
}
