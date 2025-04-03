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
import java.util.Enumeration;
import java.util.Optional;
import java.util.Vector;

public class PolicyParser {

    private final Vector<GrantEntry> grantEntries = new Vector<>();
    private TokenStream tokenStream;

    public PolicyParser() {}

    public void read(Reader policy) throws ParsingException, IOException {
        if (!(policy instanceof BufferedReader)) {
            policy = new BufferedReader(policy);
        }

        tokenStream = new TokenStream(policy);

        while (!tokenStream.isEOF()) {
            if (peek("grant")) {
                parseGrantEntry().ifPresent(this::addGrantEntry);
            }
        }
    }

    private boolean pollOnMatch(String expect) throws ParsingException, IOException {
        if (peek(expect)) {
            poll(expect);
            return true;
        }
        return false;
    }

    private boolean peek(String expected) throws IOException {
        Token token = tokenStream.peek();
        return expected.equalsIgnoreCase(token.text);
    }

    private String poll(String expected) throws IOException, ParsingException {
        Token token = tokenStream.consume();

        // Match exact keyword or symbol
        if (expected.equalsIgnoreCase("grant")
            || expected.equalsIgnoreCase("Codebase")
            || expected.equalsIgnoreCase("Permission")
            || expected.equalsIgnoreCase("{")
            || expected.equalsIgnoreCase("}")
            || expected.equalsIgnoreCase(";")
            || expected.equalsIgnoreCase(",")) {

            if (!expected.equalsIgnoreCase(token.text)) {
                throw new ParsingException(token.line, expected, token.text);
            }
            return token.text;
        }

        if (token.type == StreamTokenizer.TT_WORD || token.type == '"' || token.type == '\'') {
            return token.text;
        }

        throw new ParsingException(token.line, expected, token.text);
    }

    private Optional<GrantEntry> parseGrantEntry() throws ParsingException, IOException {
        GrantEntry grantEntry = new GrantEntry();
        poll("grant");

        while (!peek("{")) {
            if (pollOnMatch("Codebase")) {
                if (grantEntry.codeBase != null) {
                    throw new ParsingException(tokenStream.line(), "Multiple Codebase expressions");
                }

                String rawCodebase = poll(tokenStream.peek().text);
                try {
                    grantEntry.codeBase = PropertyExpander.expand(rawCodebase, true).replace(File.separatorChar, '/');
                } catch (ExpandException e) {
                    // skip this grant as expansion failed due to missing expansion property.
                    skipCurrentGrantBlock();

                    return Optional.empty();
                }
                pollOnMatch(",");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected codeBase");
            }
        }

        poll("{");

        while (!peek("}")) {
            if (peek("Permission")) {
                PermissionEntry permissionEntry = parsePermissionEntry();
                grantEntry.add(permissionEntry);
                poll(";");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected permission entry");
            }
        }

        poll("}");

        if (peek(";")) {
            poll(";");
        }

        if (grantEntry.codeBase != null) {
            grantEntry.codeBase = grantEntry.codeBase.replace(File.separatorChar, '/');
        }

        return Optional.of(grantEntry);
    }

    private void skipCurrentGrantBlock() throws IOException, ParsingException {
        // Consume until we find a matching closing '}'
        int braceDepth = 0;

        // Go until we find the initial '{'
        while (!tokenStream.isEOF()) {
            Token token = tokenStream.peek();
            if ("{".equals(token.text)) {
                braceDepth++;
                tokenStream.consume();
                break;
            }
            tokenStream.consume();
        }

        // Now consume until matching '}'
        while (braceDepth > 0 && !tokenStream.isEOF()) {
            Token token = tokenStream.consume();
            if ("{".equals(token.text)) {
                braceDepth++;
            } else if ("}".equals(token.text)) {
                braceDepth--;
            }
        }

        // Consume optional trailing semicolon
        if (peek(";")) {
            poll(";");
        }
    }

    private PermissionEntry parsePermissionEntry() throws ParsingException, IOException {
        PermissionEntry permissionEntry = new PermissionEntry();
        poll("Permission");
        permissionEntry.permission = poll(tokenStream.peek().text);

        if (isQuotedToken(tokenStream.peek())) {
            permissionEntry.name = poll(tokenStream.peek().text);
        }

        if (peek(",")) {
            poll(",");
        }

        if (isQuotedToken(tokenStream.peek())) {
            permissionEntry.action = poll(tokenStream.peek().text);
        }

        return permissionEntry;
    }

    private boolean isQuotedToken(Token token) {
        return token.type == '"' || token.type == '\'';
    }

    public void addGrantEntry(GrantEntry grantEntry) {
        grantEntries.addElement(grantEntry);
    }

    public Enumeration<GrantEntry> grantElements() {
        return grantEntries.elements();
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
