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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.script;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link ScriptMetadata} is used to store user-defined scripts
 * as part of the {@link ClusterState} using only an id as the key.
 *
 * @opensearch.internal
 */
public final class ScriptMetadata implements Metadata.Custom, Writeable, ToXContentFragment {

    /**
     * Standard deprecation logger for used to deprecate allowance of empty templates.
     */
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ScriptMetadata.class);

    /**
     * A builder used to modify the currently stored scripts data held within
     * the {@link ClusterState}.  Scripts can be added or deleted, then built
     * to generate a new {@link Map} of scripts that will be used to update
     * the current {@link ClusterState}.
     *
     * @opensearch.internal
     */
    public static final class Builder {

        private final Map<String, StoredScriptSource> scripts;

        /**
         * @param previous The current {@link ScriptMetadata} or {@code null} if there
         *                 is no existing {@link ScriptMetadata}.
         */
        public Builder(ScriptMetadata previous) {
            this.scripts = previous == null ? new HashMap<>() : new HashMap<>(previous.scripts);
        }

        /**
         * Add a new script to the existing stored scripts based on a user-specified id.  If
         * a script with the same id already exists it will be overwritten.
         * @param id The user-specified id to use for the look up.
         * @param source The user-specified stored script data held in {@link StoredScriptSource}.
         */
        public Builder storeScript(String id, StoredScriptSource source) {
            scripts.put(id, source);

            return this;
        }

        /**
         * Delete a script from the existing stored scripts based on a user-specified id.
         * @param id The user-specified id to use for the look up.
         */
        public Builder deleteScript(String id) {
            StoredScriptSource deleted = scripts.remove(id);

            if (deleted == null) {
                throw new ResourceNotFoundException("stored script [" + id + "] does not exist and cannot be deleted");
            }

            return this;
        }

        /**
         * @return A {@link ScriptMetadata} with the updated {@link Map} of scripts.
         */
        public ScriptMetadata build() {
            return new ScriptMetadata(scripts);
        }
    }

    static final class ScriptMetadataDiff implements NamedDiff<Metadata.Custom> {

        final Diff<Map<String, StoredScriptSource>> pipelines;

        ScriptMetadataDiff(ScriptMetadata before, ScriptMetadata after) {
            this.pipelines = DiffableUtils.diff(before.scripts, after.scripts, DiffableUtils.getStringKeySerializer());
        }

        ScriptMetadataDiff(StreamInput in) throws IOException {
            pipelines = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                StoredScriptSource::new,
                StoredScriptSource::readDiffFrom
            );
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new ScriptMetadata(pipelines.apply(((ScriptMetadata) part).scripts));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            pipelines.writeTo(out);
        }
    }

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetadata} adding the specified stored script.
     */
    static ScriptMetadata putStoredScript(ScriptMetadata previous, String id, StoredScriptSource source) {
        Builder builder = new Builder(previous);
        builder.storeScript(id, source);

        return builder.build();
    }

    /**
     * Convenience method to build and return a new
     * {@link ScriptMetadata} deleting the specified stored script.
     */
    static ScriptMetadata deleteStoredScript(ScriptMetadata previous, String id) {
        Builder builder = new ScriptMetadata.Builder(previous);
        builder.deleteScript(id);

        return builder.build();
    }

    /**
     * The type of {@link ClusterState} data.
     */
    public static final String TYPE = "stored_scripts";

    /**
     * This will parse XContent into {@link ScriptMetadata}.
     * <p>
     * The following format will be parsed:
     * <p>
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser, boolean)}>",
     *     "<id>" : "<{@link StoredScriptSource#fromXContent(XContentParser, boolean)}>",
     *     ...
     * }
     * }
     *
     * When loading from a source prior to 6.0, if multiple scripts
     * using the old namespace id format of [lang#id] are found to have the
     * same id but different languages an error will occur.
     */
    public static ScriptMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        String id = null;
        StoredScriptSource source;
        StoredScriptSource exists;

        Token token = parser.currentToken();

        if (token == null) {
            token = parser.nextToken();
        }

        if (token != Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [{]");
        }

        token = parser.nextToken();

        while (token != Token.END_OBJECT) {
            switch (token) {
                case FIELD_NAME:
                    id = parser.currentName();
                    break;
                case VALUE_STRING:
                    if (id == null) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]"
                        );
                    }

                    int split = id.indexOf('#');
                    String lang;

                    if (split == -1) {
                        throw new IllegalArgumentException("illegal stored script id [" + id + "], does not contain lang");
                    } else {
                        lang = id.substring(0, split);
                        id = id.substring(split + 1);
                        source = new StoredScriptSource(lang, parser.text(), Collections.emptyMap());

                        if (source.getSource().isEmpty()) {
                            if (source.getLang().equals(Script.DEFAULT_TEMPLATE_LANG)) {
                                deprecationLogger.deprecate("empty_templates", "empty templates should no longer be used");
                            } else {
                                deprecationLogger.deprecate("empty_scripts", "empty scripts should no longer be used");
                            }
                        }
                    }

                    exists = scripts.get(id);

                    if (exists == null) {
                        scripts.put(id, source);
                    } else if (exists.getLang().equals(lang) == false) {
                        throw new IllegalArgumentException(
                            "illegal stored script, id ["
                                + id
                                + "] used for multiple scripts with "
                                + "different languages ["
                                + exists.getLang()
                                + "] and ["
                                + lang
                                + "]; scripts using the old namespace "
                                + "of [lang#id] as a stored script id will have to be updated to use only the new namespace of [id]"
                        );
                    }

                    id = null;

                    break;
                case START_OBJECT:
                    if (id == null) {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "unexpected token [" + token + "], expected [<id>, <code>, {]"
                        );
                    }

                    exists = scripts.get(id);
                    source = StoredScriptSource.fromXContent(parser, true);

                    if (exists == null) {
                        // due to a bug (https://github.com/elastic/elasticsearch/issues/47593)
                        // scripts may have been retained during upgrade that include the old-style
                        // id of lang#id; these scripts are unreachable after 7.0, so they are dropped
                        if (id.contains("#") == false) {
                            scripts.put(id, source);
                        }
                    } else if (exists.getLang().equals(source.getLang()) == false) {
                        throw new IllegalArgumentException(
                            "illegal stored script, id ["
                                + id
                                + "] used for multiple scripts with "
                                + "different languages ["
                                + exists.getLang()
                                + "] and ["
                                + source.getLang()
                                + "]; scripts using the old "
                                + "namespace of [lang#id] as a stored script id will have to be updated to use only the new namespace of [id]"
                        );
                    }

                    id = null;

                    break;
                default:
                    throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "], expected [<id>, <code>, {]");
            }

            token = parser.nextToken();
        }

        return new ScriptMetadata(scripts);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new ScriptMetadataDiff(in);
    }

    private final Map<String, StoredScriptSource> scripts;

    /**
     * Standard constructor to create metadata to store scripts.
     * @param scripts The currently stored scripts.  Must not be {@code null},
     *                use and empty {@link Map} to specify there were no
     *                previously stored scripts.
     */
    ScriptMetadata(Map<String, StoredScriptSource> scripts) {
        this.scripts = Collections.unmodifiableMap(scripts);
    }

    public ScriptMetadata(StreamInput in) throws IOException {
        Map<String, StoredScriptSource> scripts = new HashMap<>();
        StoredScriptSource source;
        int size = in.readVInt();

        for (int i = 0; i < size; i++) {
            String id = in.readString();
            source = new StoredScriptSource(in);
            scripts.put(id, source);
        }

        this.scripts = Collections.unmodifiableMap(scripts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(scripts.size());

        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * This will write XContent from {@link ScriptMetadata}.  The following format will be written:
     * <p>
     * {@code
     * {
     *     "<id>" : "<{@link StoredScriptSource#toXContent(XContentBuilder, Params)}>",
     *     "<id>" : "<{@link StoredScriptSource#toXContent(XContentBuilder, Params)}>",
     *     ...
     * }
     * }
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, StoredScriptSource> entry : scripts.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }

        return builder;
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom before) {
        return new ScriptMetadataDiff((ScriptMetadata) before, this);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    /**
     * Returns the map of stored scripts.
     */
    Map<String, StoredScriptSource> getStoredScripts() {
        return scripts;
    }

    /**
     * Retrieves a stored script based on a user-specified id.
     */
    StoredScriptSource getStoredScript(String id) {
        return scripts.get(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptMetadata that = (ScriptMetadata) o;

        return scripts.equals(that.scripts);

    }

    @Override
    public int hashCode() {
        return scripts.hashCode();
    }

    @Override
    public String toString() {
        return "ScriptMetadata{" + "scripts=" + scripts + '}';
    }
}
