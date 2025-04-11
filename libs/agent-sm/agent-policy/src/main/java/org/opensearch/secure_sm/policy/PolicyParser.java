/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import org.opensearch.secure_sm.policy.PropertyExpander.ExpandException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PolicyParser {

    private PolicyParser() {}

    public static List<GrantEntry> read(Reader policy) throws ParsingException, IOException {
        final List<GrantEntry> grantEntries = new ArrayList<>();
        if (!(policy instanceof BufferedReader)) {
            policy = new BufferedReader(policy);
        }
        TokenStream tokenStream = new TokenStream(policy);
        while (!tokenStream.isEOF()) {
            if (peek(tokenStream, "grant")) {
                parseGrantEntry(tokenStream).ifPresent(grantEntries::add);
            }
        }
        return grantEntries;
    }

    private static boolean pollOnMatch(TokenStream tokenStream, String expect) throws ParsingException, IOException {
        if (peek(tokenStream, expect)) {
            poll(tokenStream, expect);
            return true;
        }
        return false;
    }

    private static boolean peek(TokenStream tokenStream, String expected) throws IOException {
        Token token = tokenStream.peek();
        return expected.equalsIgnoreCase(token.text());
    }

    private static String poll(TokenStream tokenStream, String expected) throws IOException, ParsingException {
        Token token = tokenStream.consume();

        // Match exact keyword or symbol
        if (expected.equalsIgnoreCase("grant")
            || expected.equalsIgnoreCase("Codebase")
            || expected.equalsIgnoreCase("Permission")
            || expected.equalsIgnoreCase("{")
            || expected.equalsIgnoreCase("}")
            || expected.equalsIgnoreCase(";")
            || expected.equalsIgnoreCase(",")) {

            if (!expected.equalsIgnoreCase(token.text())) {
                throw new ParsingException(token.line(), expected, token.text());
            }
            return token.text();
        }

        if (token.type() == StreamTokenizer.TT_WORD || token.type() == '"' || token.type() == '\'') {
            return token.text();
        }

        throw new ParsingException(token.line(), expected, token.text());
    }

    private static Optional<GrantEntry> parseGrantEntry(TokenStream tokenStream) throws ParsingException, IOException {
        String codeBase = null;
        List<PermissionEntry> permissionEntries = new ArrayList<>();

        poll(tokenStream, "grant");

        while (!peek(tokenStream, "{")) {
            if (pollOnMatch(tokenStream, "Codebase")) {
                if (codeBase != null) {
                    throw new ParsingException(tokenStream.line(), "Multiple Codebase expressions");
                }

                String rawCodebase = poll(tokenStream, tokenStream.peek().text());
                try {
                    codeBase = PropertyExpander.expand(rawCodebase, true).replace(File.separatorChar, '/');
                } catch (ExpandException e) {
                    // skip this grant as expansion failed due to missing expansion property.
                    skipCurrentGrantBlock(tokenStream);

                    return Optional.empty();
                }
                pollOnMatch(tokenStream, ",");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected codeBase");
            }
        }

        poll(tokenStream, "{");

        while (!peek(tokenStream, "}")) {
            if (peek(tokenStream, "Permission")) {
                permissionEntries.add(parsePermissionEntry(tokenStream));
                poll(tokenStream, ";");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected permission entry");
            }
        }

        poll(tokenStream, "}");

        if (peek(tokenStream, ";")) {
            poll(tokenStream, ";");
        }

        if (codeBase != null) {
            codeBase = codeBase.replace(File.separatorChar, '/');
        }

        return Optional.of(new GrantEntry(codeBase, permissionEntries));
    }

    private static void skipCurrentGrantBlock(TokenStream tokenStream) throws IOException, ParsingException {
        // Consume until we find a matching closing '}'
        int braceDepth = 0;

        // Go until we find the initial '{'
        while (!tokenStream.isEOF()) {
            Token token = tokenStream.peek();
            if ("{".equals(token.text())) {
                braceDepth++;
                tokenStream.consume();
                break;
            }
            tokenStream.consume();
        }

        // Now consume until matching '}'
        while (braceDepth > 0 && !tokenStream.isEOF()) {
            Token token = tokenStream.consume();
            if ("{".equals(token.text())) {
                braceDepth++;
            } else if ("}".equals(token.text())) {
                braceDepth--;
            }
        }

        // Consume optional trailing semicolon
        if (peek(tokenStream, ";")) {
            poll(tokenStream, ";");
        }
    }

    private static PermissionEntry parsePermissionEntry(TokenStream tokenStream) throws ParsingException, IOException {
        String name = null;
        String action = null;

        poll(tokenStream, "Permission");
        final String permission = poll(tokenStream, tokenStream.peek().text());

        if (isQuotedToken(tokenStream.peek())) {
            name = poll(tokenStream, tokenStream.peek().text());
        }

        if (peek(tokenStream, ",")) {
            poll(tokenStream, ",");
        }

        if (isQuotedToken(tokenStream.peek())) {
            action = poll(tokenStream, tokenStream.peek().text());
        }

        return new PermissionEntry(permission, name, action);
    }

    private static boolean isQuotedToken(Token token) {
        return token.type() == '"' || token.type() == '\'';
    }

    public static class ParsingException extends Exception {
        public ParsingException(String message) {
            super(message);
        }

        public ParsingException(int line, String expected) {
            super("line " + line + ": expected [" + expected + "]");
        }

        public ParsingException(int line, String expected, String found) {
            super("line " + line + ": expected [" + expected + "], found [" + found + "]");
        }
    }
}
