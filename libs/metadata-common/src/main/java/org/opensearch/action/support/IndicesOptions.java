/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.support;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.opensearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

/**
 * Controls how to deal with unavailable concrete indices (closed or missing), how wildcard expressions are expanded
 * to actual indices (all, closed or open indices) and how to deal with wildcard expressions that resolve to no indices.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndicesOptions implements ToXContentFragment {

    /**
     * The wildcard states.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum WildcardStates {
        OPEN,
        CLOSED,
        HIDDEN;

        public static final EnumSet<WildcardStates> NONE = EnumSet.noneOf(WildcardStates.class);

        public static EnumSet<WildcardStates> parseParameter(Object value, EnumSet<WildcardStates> defaultStates) {
            if (value == null) {
                return defaultStates;
            }

            EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);
            String[] wildcards = nodeStringArrayValue(value);
            // TODO why do we let patterns like "none,all" or "open,none,closed" get used. The location of 'none' in the array changes the
            // meaning of the resulting value
            for (String wildcard : wildcards) {
                updateSetForValue(states, wildcard);
            }

            return states;
        }

        public static XContentBuilder toXContent(EnumSet<WildcardStates> states, XContentBuilder builder) throws IOException {
            if (states.isEmpty()) {
                builder.field("expand_wildcards", "none");
            } else if (states.containsAll(EnumSet.allOf(WildcardStates.class))) {
                builder.field("expand_wildcards", "all");
            } else {
                builder.field(
                    "expand_wildcards",
                    states.stream().map(state -> state.toString().toLowerCase(Locale.ROOT)).collect(Collectors.joining(","))
                );
            }
            return builder;
        }

        private static void updateSetForValue(EnumSet<WildcardStates> states, String wildcard) {
            switch (wildcard) {
                case "open":
                    states.add(OPEN);
                    break;
                case "closed":
                    states.add(CLOSED);
                    break;
                case "hidden":
                    states.add(HIDDEN);
                    break;
                case "none":
                    states.clear();
                    break;
                case "all":
                    states.addAll(EnumSet.allOf(WildcardStates.class));
                    break;
                default:
                    throw new IllegalArgumentException("No valid expand wildcard value [" + wildcard + "]");
            }
        }
    }

    /**
     * The options.
     *
     * @opensearch.internal
     */
    public enum Option {
        IGNORE_UNAVAILABLE,
        IGNORE_ALIASES,
        ALLOW_NO_INDICES,
        FORBID_ALIASES_TO_MULTIPLE_INDICES,
        FORBID_CLOSED_INDICES,
        IGNORE_THROTTLED;

        public static final EnumSet<Option> NONE = EnumSet.noneOf(Option.class);
    }

    public static final IndicesOptions STRICT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    public static final IndicesOptions LENIENT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.IGNORE_UNAVAILABLE),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_CLOSED_HIDDEN = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.of(WildcardStates.OPEN, WildcardStates.HIDDEN)
    );
    public static final IndicesOptions STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED = new IndicesOptions(
        EnumSet.of(Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES, Option.IGNORE_THROTTLED),
        EnumSet.of(WildcardStates.OPEN)
    );
    public static final IndicesOptions STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED = new IndicesOptions(
        EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES, Option.FORBID_CLOSED_INDICES),
        EnumSet.noneOf(WildcardStates.class)
    );

    private final EnumSet<Option> options;
    private final EnumSet<WildcardStates> expandWildcards;

    public IndicesOptions(EnumSet<Option> options, EnumSet<WildcardStates> expandWildcards) {
        this.options = options;
        this.expandWildcards = expandWildcards;
    }

    /**
     * @return Whether specified concrete indices should be ignored when unavailable (missing or closed)
     */
    public boolean ignoreUnavailable() {
        return options.contains(Option.IGNORE_UNAVAILABLE);
    }

    /**
     * @return Whether to ignore if a wildcard expression resolves to no concrete indices.
     *         The `_all` string or empty list of indices count as wildcard expressions too.
     *         Also when an alias points to a closed index this option decides if no concrete indices
     *         are allowed.
     */
    public boolean allowNoIndices() {
        return options.contains(Option.ALLOW_NO_INDICES);
    }

    /**
     * @return Whether wildcard expressions should get expanded to open indices
     */
    public boolean expandWildcardsOpen() {
        return expandWildcards.contains(WildcardStates.OPEN);
    }

    /**
     * @return Whether wildcard expressions should get expanded to closed indices
     */
    public boolean expandWildcardsClosed() {
        return expandWildcards.contains(WildcardStates.CLOSED);
    }

    /**
     * @return Whether wildcard expressions should get expanded to hidden indices
     */
    public boolean expandWildcardsHidden() {
        return expandWildcards.contains(WildcardStates.HIDDEN);
    }

    /**
     * @return Whether execution on closed indices is allowed.
     */
    public boolean forbidClosedIndices() {
        return options.contains(Option.FORBID_CLOSED_INDICES);
    }

    /**
     * @return whether aliases pointing to multiple indices are allowed
     */
    public boolean allowAliasesToMultipleIndices() {
        // true is default here, for bw comp we keep the first 16 values
        // in the array same as before + the default value for the new flag
        return options.contains(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES) == false;
    }

    /**
     * @return whether aliases should be ignored (when resolving a wildcard)
     */
    public boolean ignoreAliases() {
        return options.contains(Option.IGNORE_ALIASES);
    }

    /**
     * @return whether indices that are marked as throttled should be ignored
     */
    public boolean ignoreThrottled() {
        return options.contains(Option.IGNORE_THROTTLED);
    }

    /**
     * @return a copy of the {@link WildcardStates} that these indices options will expand to
     */
    public EnumSet<WildcardStates> getExpandWildcards() {
        return EnumSet.copyOf(expandWildcards);
    }

    public void writeIndicesOptions(StreamOutput out) throws IOException {
        EnumSet<Option> options = this.options;
        out.writeEnumSet(options);
        out.writeEnumSet(expandWildcards);
    }

    public static IndicesOptions readIndicesOptions(StreamInput in) throws IOException {
        EnumSet<Option> options = in.readEnumSet(Option.class);
        EnumSet<WildcardStates> states = in.readEnumSet(WildcardStates.class);
        return new IndicesOptions(options, states);
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices
    ) {
        return fromOptions(ignoreUnavailable, allowNoIndices, expandToOpenIndices, expandToClosedIndices, false);
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            expandToHiddenIndices,
            true,
            false,
            false,
            false
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        IndicesOptions defaultOptions
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            defaultOptions.expandWildcardsHidden(),
            defaultOptions.allowAliasesToMultipleIndices(),
            defaultOptions.forbidClosedIndices(),
            defaultOptions.ignoreAliases(),
            defaultOptions.ignoreThrottled()
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        return fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            expandToOpenIndices,
            expandToClosedIndices,
            false,
            allowAliasesToMultipleIndices,
            forbidClosedIndices,
            ignoreAliases,
            ignoreThrottled
        );
    }

    public static IndicesOptions fromOptions(
        boolean ignoreUnavailable,
        boolean allowNoIndices,
        boolean expandToOpenIndices,
        boolean expandToClosedIndices,
        boolean expandToHiddenIndices,
        boolean allowAliasesToMultipleIndices,
        boolean forbidClosedIndices,
        boolean ignoreAliases,
        boolean ignoreThrottled
    ) {
        final EnumSet<Option> opts = EnumSet.noneOf(Option.class);
        final EnumSet<WildcardStates> wildcards = EnumSet.noneOf(WildcardStates.class);

        if (ignoreUnavailable) {
            opts.add(Option.IGNORE_UNAVAILABLE);
        }
        if (allowNoIndices) {
            opts.add(Option.ALLOW_NO_INDICES);
        }
        if (expandToOpenIndices) {
            wildcards.add(WildcardStates.OPEN);
        }
        if (expandToClosedIndices) {
            wildcards.add(WildcardStates.CLOSED);
        }
        if (expandToHiddenIndices) {
            wildcards.add(WildcardStates.HIDDEN);
        }
        if (allowAliasesToMultipleIndices == false) {
            opts.add(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES);
        }
        if (forbidClosedIndices) {
            opts.add(Option.FORBID_CLOSED_INDICES);
        }
        if (ignoreAliases) {
            opts.add(Option.IGNORE_ALIASES);
        }
        if (ignoreThrottled) {
            opts.add(Option.IGNORE_THROTTLED);
        }
        return new IndicesOptions(opts, wildcards);
    }

    public static IndicesOptions fromRequest(RestRequest request, IndicesOptions defaultSettings) {
        return fromParameters(
            request.param("expand_wildcards"),
            request.param("ignore_unavailable"),
            request.param("allow_no_indices"),
            request.param("ignore_throttled"),
            defaultSettings
        );
    }

    public static IndicesOptions fromMap(Map<String, Object> map, IndicesOptions defaultSettings) {
        return fromParameters(
            map.containsKey("expand_wildcards") ? map.get("expand_wildcards") : map.get("expandWildcards"),
            map.containsKey("ignore_unavailable") ? map.get("ignore_unavailable") : map.get("ignoreUnavailable"),
            map.containsKey("allow_no_indices") ? map.get("allow_no_indices") : map.get("allowNoIndices"),
            map.containsKey("ignore_throttled") ? map.get("ignore_throttled") : map.get("ignoreThrottled"),
            defaultSettings
        );
    }

    /**
     * Returns true if the name represents a valid name for one of the indices option
     * false otherwise
     */
    public static boolean isIndicesOptions(String name) {
        return "expand_wildcards".equals(name)
            || "expandWildcards".equals(name)
            || "ignore_unavailable".equals(name)
            || "ignoreUnavailable".equals(name)
            || "ignore_throttled".equals(name)
            || "ignoreThrottled".equals(name)
            || "allow_no_indices".equals(name)
            || "allowNoIndices".equals(name);
    }

    public static IndicesOptions fromParameters(
        Object wildcardsString,
        Object ignoreUnavailableString,
        Object allowNoIndicesString,
        Object ignoreThrottled,
        IndicesOptions defaultSettings
    ) {
        if (wildcardsString == null && ignoreUnavailableString == null && allowNoIndicesString == null && ignoreThrottled == null) {
            return defaultSettings;
        }

        EnumSet<WildcardStates> wildcards = WildcardStates.parseParameter(wildcardsString, defaultSettings.expandWildcards);

        // note that allowAliasesToMultipleIndices is not exposed, always true (only for internal use)
        return fromOptions(
            nodeBooleanValue(ignoreUnavailableString, "ignore_unavailable", defaultSettings.ignoreUnavailable()),
            nodeBooleanValue(allowNoIndicesString, "allow_no_indices", defaultSettings.allowNoIndices()),
            wildcards.contains(WildcardStates.OPEN),
            wildcards.contains(WildcardStates.CLOSED),
            wildcards.contains(WildcardStates.HIDDEN),
            defaultSettings.allowAliasesToMultipleIndices(),
            defaultSettings.forbidClosedIndices(),
            defaultSettings.ignoreAliases(),
            nodeBooleanValue(ignoreThrottled, "ignore_throttled", defaultSettings.ignoreThrottled())
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("expand_wildcards");
        for (WildcardStates expandWildcard : expandWildcards) {
            builder.value(expandWildcard.toString().toLowerCase(Locale.ROOT));
        }
        builder.endArray();
        builder.field("ignore_unavailable", ignoreUnavailable());
        builder.field("allow_no_indices", allowNoIndices());
        builder.field("ignore_throttled", ignoreThrottled());
        return builder;
    }

    private static final ParseField EXPAND_WILDCARDS_FIELD = new ParseField("expand_wildcards");
    private static final ParseField IGNORE_UNAVAILABLE_FIELD = new ParseField("ignore_unavailable");
    private static final ParseField IGNORE_THROTTLED_FIELD = new ParseField("ignore_throttled");
    private static final ParseField ALLOW_NO_INDICES_FIELD = new ParseField("allow_no_indices");

    public static IndicesOptions fromXContent(XContentParser parser) throws IOException {
        EnumSet<WildcardStates> wildcardStates = null;
        Boolean allowNoIndices = null;
        Boolean ignoreUnavailable = null;
        boolean ignoreThrottled = false;
        Token token = parser.currentToken() == Token.START_OBJECT ? parser.currentToken() : parser.nextToken();
        String currentFieldName = null;
        if (token != Token.START_OBJECT) {
            throw new OpenSearchParseException("expected START_OBJECT as the token but was " + token);
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == Token.START_ARRAY) {
                if (EXPAND_WILDCARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (wildcardStates == null) {
                        wildcardStates = EnumSet.noneOf(WildcardStates.class);
                        while ((token = parser.nextToken()) != Token.END_ARRAY) {
                            if (token.isValue()) {
                                WildcardStates.updateSetForValue(wildcardStates, parser.text());
                            } else {
                                throw new OpenSearchParseException(
                                    "expected values within array for " + EXPAND_WILDCARDS_FIELD.getPreferredName()
                                );
                            }
                        }
                    } else {
                        throw new OpenSearchParseException("already parsed expand_wildcards");
                    }
                } else {
                    throw new OpenSearchParseException(
                        EXPAND_WILDCARDS_FIELD.getPreferredName() + " is the only field that is an array in IndicesOptions"
                    );
                }
            } else if (token.isValue()) {
                if (EXPAND_WILDCARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (wildcardStates == null) {
                        wildcardStates = EnumSet.noneOf(WildcardStates.class);
                        WildcardStates.updateSetForValue(wildcardStates, parser.text());
                    } else {
                        throw new OpenSearchParseException("already parsed expand_wildcards");
                    }
                } else if (IGNORE_UNAVAILABLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnavailable = parser.booleanValue();
                } else if (ALLOW_NO_INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    allowNoIndices = parser.booleanValue();
                } else if (IGNORE_THROTTLED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreThrottled = parser.booleanValue();
                } else {
                    throw new OpenSearchParseException(
                        "could not read indices options. unexpected index option [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new OpenSearchParseException("could not read indices options. unexpected object field [" + currentFieldName + "]");
            }
        }

        if (wildcardStates == null) {
            throw new OpenSearchParseException("indices options xcontent did not contain " + EXPAND_WILDCARDS_FIELD.getPreferredName());
        }
        if (ignoreUnavailable == null) {
            throw new OpenSearchParseException("indices options xcontent did not contain " + IGNORE_UNAVAILABLE_FIELD.getPreferredName());
        }
        if (allowNoIndices == null) {
            throw new OpenSearchParseException("indices options xcontent did not contain " + ALLOW_NO_INDICES_FIELD.getPreferredName());
        }

        return IndicesOptions.fromOptions(
            ignoreUnavailable,
            allowNoIndices,
            wildcardStates.contains(WildcardStates.OPEN),
            wildcardStates.contains(WildcardStates.CLOSED),
            wildcardStates.contains(WildcardStates.HIDDEN),
            true,
            false,
            false,
            ignoreThrottled
        );
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpen() {
        return STRICT_EXPAND_OPEN;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices, includes hidden
     *         indices, and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandOpenHidden() {
        return STRICT_EXPAND_OPEN_HIDDEN;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error) and forbids the
     *         use of closed indices by throwing an error.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosed() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED;
    }

    /**
     * @return indices options that requires every specified index to exist, expands wildcards only to open indices,
     *         allows that no indices are resolved from wildcard expressions (not returning an error),
     *         forbids the use of closed indices by throwing an error and ignores indices that are throttled.
     */
    public static IndicesOptions strictExpandOpenAndForbidClosedIgnoreThrottled() {
        return STRICT_EXPAND_OPEN_FORBID_CLOSED_IGNORE_THROTTLED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpand() {
        return STRICT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices option that requires every specified index to exist, expands wildcards to both open and closed indices, includes
     *         hidden indices, and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions strictExpandHidden() {
        return STRICT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    /**
     * @return indices option that requires each specified index or alias to exist, doesn't expand wildcards and
     * throws error if any of the aliases resolves to multiple indices
     */
    public static IndicesOptions strictSingleIndexNoExpandForbidClosed() {
        return STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards only to open indices and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpen() {
        return LENIENT_EXPAND_OPEN;
    }

    /**
     * @return indices options that ignores unavailable indices, expands wildcards to open and hidden indices, and
     *         allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandOpenHidden() {
        return LENIENT_EXPAND_OPEN_HIDDEN;
    }

    /**
     * @return indices options that ignores unavailable indices,  expands wildcards to both open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpand() {
        return LENIENT_EXPAND_OPEN_CLOSED;
    }

    /**
     * @return indices options that ignores unavailable indices,  expands wildcards to all open and closed
     * indices and allows that no indices are resolved from wildcard expressions (not returning an error).
     */
    public static IndicesOptions lenientExpandHidden() {
        return LENIENT_EXPAND_OPEN_CLOSED_HIDDEN;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj.getClass() != getClass()) {
            return false;
        }

        IndicesOptions other = (IndicesOptions) obj;
        return options.equals(other.options) && expandWildcards.equals(other.expandWildcards);
    }

    @Override
    public int hashCode() {
        int result = options.hashCode();
        return 31 * result + expandWildcards.hashCode();
    }

    @Override
    public String toString() {
        return "IndicesOptions["
            + "ignore_unavailable="
            + ignoreUnavailable()
            + ", allow_no_indices="
            + allowNoIndices()
            + ", expand_wildcards_open="
            + expandWildcardsOpen()
            + ", expand_wildcards_closed="
            + expandWildcardsClosed()
            + ", expand_wildcards_hidden="
            + expandWildcardsHidden()
            + ", allow_aliases_to_multiple_indices="
            + allowAliasesToMultipleIndices()
            + ", forbid_closed_indices="
            + forbidClosedIndices()
            + ", ignore_aliases="
            + ignoreAliases()
            + ", ignore_throttled="
            + ignoreThrottled()
            + ']';
    }
}
