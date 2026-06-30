/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.StoredScriptId;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.script.Script.CONTENT_TYPE_OPTION;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_LANG;

/**
 * Utility class for converting SourceConfig Protocol Buffers to FetchSourceContext objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding OpenSearch objects.
 */
public class ScriptProtoUtils {

    private ScriptProtoUtils() {
        // Utility class, no instances
    }

    /**
     *
     * Convenience method to call {@link ScriptProtoUtils#parseFromProtoRequest(org.opensearch.protobufs.Script, String)}
     * Similar to {@link Script#parse(XContentParser)}
     *
     * @param script
     * @return
     */
    public static Script parseFromProtoRequest(org.opensearch.protobufs.Script script) {
        return parseFromProtoRequest(script, DEFAULT_SCRIPT_LANG);
    }

    /**
     * Converts a Script Protocol Buffer to a Script object.
     * Similar to {@link Script#parse(XContentParser, String)}, which internally calls Script#build().
     *
     * @param script the Protocol Buffer Script to convert
     * @param defaultLang the default script language to use if not specified
     * @return the converted Script object
     */
    private static Script parseFromProtoRequest(org.opensearch.protobufs.Script script, String defaultLang) {
        Objects.requireNonNull(defaultLang);

        if (script.hasInline()) {
            return parseInlineScript(script.getInline(), defaultLang);
        } else if (script.hasStored()) {
            return parseStoredScriptId(script.getStored());
        } else {
            throw new UnsupportedOperationException("No valid script type detected");
        }
    }

    /**
     * Parses a protobuf InlineScript to a Script object
     *
     * @param inlineScript the Protocol Buffer InlineScript to convert
     * @param defaultLang the default script language to use if not specified
     * @return the converted Script object
     */
    public static Script parseInlineScript(InlineScript inlineScript, String defaultLang) {

        ScriptType type = ScriptType.INLINE;

        String lang = inlineScript.hasLang() ? parseScriptLanguage(inlineScript.getLang(), defaultLang) : defaultLang;
        String idOrCode = inlineScript.getSource();

        Map<String, String> options = inlineScript.getOptionsMap();
        if (options.size() > 1 || options.size() == 1 && options.get(CONTENT_TYPE_OPTION) == null) {
            throw new IllegalArgumentException("illegal compiler options [" + options + "] specified");
        }

        Map<String, Object> params = inlineScript.hasParams()
            ? ObjectMapProtoUtils.fromProto(inlineScript.getParams())
            : Collections.emptyMap();

        return new Script(type, lang, idOrCode, options, params);
    }

    /**
     * Parses a protobuf StoredScriptId to a Script object
     *
     * @param storedScriptId the Protocol Buffer StoredScriptId to convert
     * @return the converted Script object
     */
    public static Script parseStoredScriptId(StoredScriptId storedScriptId) {
        ScriptType type = ScriptType.STORED;
        String lang = null;
        String idOrCode = storedScriptId.getId();
        Map<String, String> options = null;
        Map<String, Object> params = storedScriptId.hasParams()
            ? ObjectMapProtoUtils.fromProto(storedScriptId.getParams())
            : Collections.emptyMap();

        return new Script(type, lang, idOrCode, options, params);
    }

    /**
     * Parses a protobuf ScriptLanguage to a String representation
     *
     * @param language the Protocol Buffer ScriptLanguage to convert
     * @param defaultLang the default script language to use if not specified
     * @return the string representation of the script language
     * @throws UnsupportedOperationException if no language was specified
     */
    public static String parseScriptLanguage(ScriptLanguage language, String defaultLang) {
        if (language.hasCustom()) {
            return language.getCustom();
        }
        switch (language.getBuiltin()) {
            case BUILTIN_SCRIPT_LANGUAGE_EXPRESSION:
                return "expression";
            case BUILTIN_SCRIPT_LANGUAGE_JAVA:
                return "java";
            case BUILTIN_SCRIPT_LANGUAGE_MUSTACHE:
                return "mustache";
            case BUILTIN_SCRIPT_LANGUAGE_PAINLESS:
                return "painless";
            case BUILTIN_SCRIPT_LANGUAGE_UNSPECIFIED:
            default:
                return defaultLang;
        }
    }
}
