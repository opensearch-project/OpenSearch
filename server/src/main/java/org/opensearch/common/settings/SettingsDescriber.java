/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Nullable;
import org.opensearch.common.regex.Regex;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Derives definition metadata for registered settings from a setting registry. Shared by the cluster and index settings
 * describe APIs: each passes its own registry ({@link ClusterSettings} or {@link IndexScopedSettings}) and the result
 * depends on nothing but that registry, the node's {@link SettingsFilter} and the {@link Setting} objects themselves. A
 * configured value is never read.
 * @opensearch.internal
 */
public final class SettingsDescriber {
    /**
     * The only admitted wildcard shape: one or more literal dotted segments followed by a trailing {@code .*}. The
     * character class is the one {@link AbstractScopedSettings#isValidKey} accepts, so a prefix is built from the same
     * characters a real key may contain. {@code *}, {@code *.x}, {@code a.*.b}, {@code a.?.*} and {@code a.**} all fail.
     */
    private static final Pattern PREFIX_WILDCARD = Pattern.compile("^(?:[-\\w]+[.])+\\*$");

    private SettingsDescriber() {}

    /**
     * Validates the names of a describe request: at least one, none blank, and each either an exact setting name or a single
     * trailing-wildcard prefix such as {@code cluster.routing.allocation.*}.
     *
     * @param example a prefix from the caller's registry, quoted in the error message for a rejected wildcard shape
     * @return the accumulated validation errors, or {@code null} when the names are acceptable
     */
    @Nullable
    public static ActionRequestValidationException validateNames(String[] names, String example) {
        ActionRequestValidationException exception = null;
        if (names.length == 0) {
            exception = addValidationError("settings must contain at least one setting name or prefix", exception);
        }
        for (String name : names) {
            if (name == null || name.isBlank()) {
                exception = addValidationError("settings must not contain blank names", exception);
                break;
            }
            boolean wildcard = Regex.isSimpleMatchPattern(name);
            if (wildcard && PREFIX_WILDCARD.matcher(name).matches() == false) {
                exception = addValidationError(
                    "settings may use a single trailing wildcard such as " + example + "; got [" + name + "]",
                    exception
                );
                break;
            }
            if (wildcard == false && AbstractScopedSettings.isValidKey(name) == false) {
                exception = addValidationError("settings must contain exact setting names; got [" + name + "]", exception);
                break;
            }
        }
        return exception;
    }

    /**
     * Describes the requested names against a registry. Exact names are looked up as given; prefixes are expanded to the
     * concrete keys registered under them. Names that are unknown, filtered or not canonical are omitted.
     *
     * @return one entry per described setting, in no particular order
     */
    public static Map<String, SettingMetadata> describe(String[] names, AbstractScopedSettings registry, SettingsFilter filter) {
        // Expand each prefix to the keys registered under it, then pass every candidate name (exact or expanded)
        // through the same filter gate. Names are filtered per key rather than through a Settings.Builder: the builder
        // rewrites legacy list keys (a.b.0 becomes a.b) and rejects a.b alongside a.b.0, which would misdescribe one
        // request and turn another into a 500. The configured value is never read. Order is irrelevant here; the
        // responses sort by key.
        var candidates = new HashSet<String>();
        // Dedupe first so a prefix repeated in the request is expanded (a full registry scan) only once.
        for (String name : new HashSet<>(Arrays.asList(names))) {
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
        var entries = new HashMap<String, SettingMetadata>();
        for (String name : candidates) {
            if (filter.isFiltered(name)) {
                continue;
            }
            Setting<?> setting = registry.get(name);
            // Omission deliberately makes an unknown name indistinguishable from a filtered one. The registry resolves
            // variants it does not report, such as a list key's legacy indexed form or a suffix appended to a concrete affix
            // key; only canonical names are described, so the response never carries a name that is not a setting.
            if (setting != null && setting.isFiltered() == false && registry.isCanonicalKey(name)) {
                entries.put(name, metadataOf(setting));
            }
        }
        return entries;
    }

    /**
     * Describes a single registered setting. The default is the setting's declared default evaluated against
     * {@link Settings#EMPTY}, so a default that depends on another setting is computed from that setting's declared default,
     * not from this node's configuration. A default function is free to read other state, though: the three
     * {@code index.merge.scheduler.*} settings declare their default as the current value of the matching
     * {@code cluster.index.merge.scheduler.*} cluster setting, so for them the reported default follows that cluster setting,
     * exactly as {@code include_defaults} reports it. That value is pushed only to nodes hosting at least one index, so for those
     * three settings an index-free node keeps reporting the processor-derived default and the answer depends on the receiving node.
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
    public static SettingValidation validationOf(Setting<?> setting) {
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
