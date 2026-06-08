/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent.yaml;

import java.io.Reader;
import java.util.Set;

import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.events.ScalarEvent;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.io.IOContext;
import tools.jackson.core.util.BufferRecycler;
import tools.jackson.dataformat.yaml.YAMLParser;

/**
 * Extends Jackson 3.x's {@link YAMLParser} to restore YAML 1.1 boolean handling
 * that was present in Jackson 2.x (via SnakeYAML 1.x).
 *
 * <p>Jackson 3.x only recognizes lowercase {@code true}/{@code false} as boolean tokens.
 * The other forms from SnakeYAML's {@code Resolver.BOOL} regex
 * ({@code yes/Yes/YES, no/No/NO, True/TRUE, False/FALSE, on/On/ON, off/Off/OFF})
 * are emitted as {@code VALUE_STRING}. This subclass checks whether the scalar is
 * unquoted (plain) and matches one of these forms, and if so, returns the appropriate
 * boolean token — matching the behavior of Jackson 2.x.
 *
 * <p>Quoted scalars (e.g. {@code "no"}) are left as {@code VALUE_STRING}, preserving
 * the YAML convention that quoting forces a string interpretation.
 */
class OpenSearchYamlParser extends YAMLParser {

    private static final Set<String> YAML_BOOLEAN_TRUE = Set.of("True", "TRUE", "yes", "Yes", "YES", "on", "On", "ON");
    private static final Set<String> YAML_BOOLEAN_FALSE = Set.of("False", "FALSE", "no", "No", "NO", "off", "Off", "OFF");

    OpenSearchYamlParser(
        ObjectReadContext readCtxt,
        IOContext ioCtxt,
        BufferRecycler br,
        int stdFeatures,
        int formatFeatures,
        LoadSettings loadSettings,
        Reader reader
    ) {
        super(readCtxt, ioCtxt, br, stdFeatures, formatFeatures, loadSettings, reader);
    }

    @Override
    protected JsonToken _decodeScalar(ScalarEvent scalar) throws JacksonException {
        JsonToken token = super._decodeScalar(scalar);
        if (token == JsonToken.VALUE_STRING && scalar.isPlain()) {
            String value = scalar.getValue();
            if (YAML_BOOLEAN_TRUE.contains(value)) {
                return JsonToken.VALUE_TRUE;
            } else if (YAML_BOOLEAN_FALSE.contains(value)) {
                return JsonToken.VALUE_FALSE;
            }
        }
        return token;
    }
}
