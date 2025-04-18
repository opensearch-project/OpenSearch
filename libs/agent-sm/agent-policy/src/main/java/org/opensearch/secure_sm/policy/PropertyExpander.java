/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PropertyExpander {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\$\\{\\{(?<escaped>.*?)}}|\\$\\{(?<normal>.*?)}");

    public static class ExpandException extends GeneralSecurityException {
        private static final long serialVersionUID = -1L;

        public ExpandException(String message) {
            super(message);
        }
    }

    public static String expand(String value) throws ExpandException {
        return expand(value, false);
    }

    public static String expand(String value, boolean encodeURL) throws ExpandException {
        if (value == null || !value.contains("${")) {
            return value;
        }

        Matcher matcher = PLACEHOLDER_PATTERN.matcher(value);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String replacement = handleMatch(matcher, encodeURL);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String handleMatch(Matcher match, boolean encodeURL) throws ExpandException {
        String escaped = match.group("escaped");
        if (escaped != null) {
            return "${{" + escaped + "}}";
        }

        String placeholder = match.group("normal");
        return expandPlaceholder(placeholder, encodeURL);
    }

    private static String expandPlaceholder(String placeholder, boolean encodeURL) throws ExpandException {
        return switch (placeholder) {
            case "/" -> String.valueOf(File.separatorChar);
            default -> {
                String value = System.getProperty(placeholder);
                if (value == null) {
                    throw new ExpandException("Unable to expand property: " + placeholder);
                }
                yield encodeURL ? encodeValue(value) : value;
            }
        };
    }

    private static String encodeValue(String value) {
        try {
            URI uri = new URI(value);
            return uri.isAbsolute() ? value : URLEncoder.encode(value, StandardCharsets.UTF_8);
        } catch (URISyntaxException e) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8);
        }
    }
}
