/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.settings;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingMetadata;
import org.opensearch.action.admin.cluster.settings.ClusterDescribeSettingsResponse.SettingValidation;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.pagination.PageToken;
import org.opensearch.action.pagination.PaginationStrategy;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.AbstractScopedSettings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;
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
        // Expand each prefix to the keys registered under it, then pass every candidate name (exact or expanded)
        // through the same filter gate. Names are matched against the filter patterns directly rather than through a
        // Settings.Builder: the builder rewrites legacy list keys (a.b.0 becomes a.b) and rejects a.b alongside a.b.0,
        // which would misdescribe one request and turn another into a 500. The configured value is never read.
        var candidates = new TreeSet<String>();
        // Dedupe first so a prefix repeated in the request is expanded (a full registry scan) only once.
        for (String name : new LinkedHashSet<>(Arrays.asList(request.names()))) {
            if (Regex.isSimpleMatchPattern(name)) {
                for (Setting<?> setting : registry.getMatching(name)) {
                    // Keystore settings are never listed by include_defaults either; skipping them keeps this a strict
                    // subset of what that API exposes. Requested by exact name they still describe as today. (The
                    // insecure fallback form is a plain Setting declared Filtered, so the gate below removes it.)
                    if (setting instanceof SecureSetting == false) {
                        candidates.add(setting.getKey());
                    }
                }
            } else {
                candidates.add(name);
            }
        }
        // Sorted: the page cursor is defined over this order. Only the visible Setting objects are collected here;
        // metadata is derived in page() for the entries that end up on the page, so the cost is proportional to the page
        // rather than to the whole expansion.
        var visible = new TreeMap<String, Setting<?>>();
        for (String name : candidates) {
            if (isHiddenByFilter(filter, name)) {
                continue;
            }
            Setting<?> setting = registry.get(name);
            // Omission deliberately makes an unknown name indistinguishable from a filtered one. The name must also be the
            // setting's declared key: the registry resolves variants such as a list key's legacy indexed form (a.b.0), and
            // the response reports only canonical names.
            if (setting != null && setting.isFiltered() == false && name.equals(setting.getKey())) {
                visible.put(name, setting);
            }
        }
        return page(visible, request.pageParams());
    }

    /** The same rule {@link SettingsFilter#filter} applies: literal patterns match the key exactly, wildcard patterns by glob. */
    private static boolean isHiddenByFilter(SettingsFilter filter, String key) {
        for (String pattern : filter.getPatterns()) {
            if (Regex.isSimpleMatchPattern(pattern) ? Regex.simpleMatch(pattern, key) : pattern.equals(key)) {
                return true;
            }
        }
        return false;
    }

    private static final String TOKEN_SEPARATOR = "|";
    private static final String PAGINATED_ENTITY = "settings";

    /**
     * Slices the sorted result into one page and derives metadata for exactly the entries on it, so the cost of a request
     * is proportional to {@code size} rather than to the number of settings the request matched. The cursor is a keyset token, {@code Base64(sort|lastKey)}: the next page
     * is "keys strictly after {@code lastKey}" in the requested order, so no entry is skipped or repeated, and the
     * embedded sort means a token cannot be replayed with the other direction. The token is bound to the sort order,
     * not to the requested names: sent with a different {@code settings} list it simply continues that list from the
     * same key, which is well defined and leaks nothing. {@code |} cannot appear in a setting key (see
     * {@link AbstractScopedSettings#isValidKey}), so the separator is unambiguous. The token holds a key the caller was
     * already shown, never a value. {@code null} page parameters mean the defaults.
     */
    private static ClusterDescribeSettingsResponse page(TreeMap<String, Setting<?>> all, @Nullable PageParams params) {
        String sort = params == null ? PageParams.PARAM_ASC_SORT_VALUE : params.getSort();
        int size = params == null ? ClusterDescribeSettingsRequest.DEFAULT_PAGE_SIZE : params.getSize();
        // validate() enforces the bounds for every request; this guards direct callers of describe() such as tests.
        assert size >= 1 && size <= ClusterDescribeSettingsRequest.MAX_PAGE_SIZE : "page size out of range: " + size;
        NavigableMap<String, Setting<?>> ordered = PageParams.PARAM_DESC_SORT_VALUE.equals(sort) ? all.descendingMap() : all;
        // An empty token means "first page", as it does for the other paginated APIs (see IndexPaginationStrategy); a client
        // that templates ?next_token=${token} before it has one should not be rejected.
        if (params != null && params.getRequestedToken() != null && params.getRequestedToken().isEmpty() == false) {
            ordered = ordered.tailMap(decodeLastKey(params.getRequestedToken(), sort), false);
        }
        var pageEntries = new LinkedHashMap<String, SettingMetadata>();
        String lastKey = null;
        for (var entry : ordered.entrySet()) {
            if (pageEntries.size() == size) {
                // A further entry exists, so this page is not the last one.
                String token = PaginationStrategy.encryptStringToken(sort + TOKEN_SEPARATOR + lastKey);
                return new ClusterDescribeSettingsResponse(pageEntries, new PageToken(token, PAGINATED_ENTITY));
            }
            pageEntries.put(entry.getKey(), metadataOf(entry.getValue()));
            lastKey = entry.getKey();
        }
        return new ClusterDescribeSettingsResponse(pageEntries, null);
    }

    private static String decodeLastKey(String token, String sort) {
        // Throws the standard tainted-token error when the string is not valid Base64.
        String decoded = PaginationStrategy.decryptStringToken(token);
        int separator = decoded.indexOf(TOKEN_SEPARATOR);
        if (separator <= 0) {
            throw new OpenSearchParseException(PaginationStrategy.INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }
        String tokenSort = decoded.substring(0, separator);
        String lastKey = decoded.substring(separator + 1);
        // A key that is not even a well-formed setting name cannot have come from a page this node produced.
        if (isSort(tokenSort) == false || AbstractScopedSettings.isValidKey(lastKey) == false) {
            throw new OpenSearchParseException(PaginationStrategy.INCORRECT_TAINTED_NEXT_TOKEN_ERROR_MESSAGE);
        }
        // A genuine token sent with the other sort (typically a client that dropped sort=desc on the follow-up request,
        // so the default asc applied) is a usage error, not tampering, and gets a message that says what to fix.
        if (tokenSort.equals(sort) == false) {
            throw new OpenSearchParseException(
                "Parameter [next_token] was issued for [sort=" + tokenSort + "] and cannot be used with [sort=" + sort + "]."
            );
        }
        return lastKey;
    }

    private static boolean isSort(String value) {
        return PageParams.PARAM_ASC_SORT_VALUE.equals(value) || PageParams.PARAM_DESC_SORT_VALUE.equals(value);
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
        // The Setting constructor only asserts that the PARSED default is non-null, and only with assertions enabled, so
        // a null raw default is guarded here rather than assumed away. Group settings declare an empty default; an empty
        // string carries no information for any setting, so it is reported the same way as no default.
        SettingMetadata metadata = defaultValue == null || defaultValue.isEmpty()
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
