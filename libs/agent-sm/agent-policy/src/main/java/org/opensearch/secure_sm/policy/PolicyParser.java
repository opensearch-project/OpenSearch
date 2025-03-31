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
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
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
                GrantEntry ge = parseGrantEntry();
                if (ge != null) {
                    add(ge);
                }
            } else {
                throw new ParsingException(tokenStream.line(), "Expected 'grant'");
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

    private GrantEntry parseGrantEntry() throws ParsingException, IOException {
        GrantEntry entry = new GrantEntry();
        poll("grant");

        while (!peek("{")) {
            if (pollOnMatch("Codebase")) {
                if (entry.codeBase != null) {
                    throw new ParsingException(tokenStream.line(), "Multiple Codebase expressions");
                }

                String rawCodebase = poll(tokenStream.peek().text);
                try {
                    entry.codeBase = PropertyExpander.expand(rawCodebase, true).replace(File.separatorChar, '/');
                } catch (ExpandException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                pollOnMatch(",");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected codeBase");
            }
        }

        poll("{");

        while (!peek("}")) {
            if (peek("Permission")) {
                PermissionEntry pe = parsePermissionEntry();
                entry.add(pe);
                poll(";");
            } else {
                throw new ParsingException(tokenStream.line(), "Expected permission entry");
            }
        }

        poll("}");

        if (peek(";")) {
            poll(";");
        }

        if (entry.codeBase != null) {
            entry.codeBase = entry.codeBase.replace(File.separatorChar, '/');
        }

        return entry;
    }

    private PermissionEntry parsePermissionEntry() throws ParsingException, IOException {
        PermissionEntry entry = new PermissionEntry();
        poll("Permission");
        entry.permission = poll(tokenStream.peek().text);

        if (isQuotedToken(tokenStream.peek())) {
            entry.name = poll(tokenStream.peek().text);
            // Might need expansion...
        }

        if (peek(",")) {
            poll(",");
        }

        if (isQuotedToken(tokenStream.peek())) {
            entry.action = poll(tokenStream.peek().text);
        }

        return entry;
    }

    private boolean isQuotedToken(Token token) {
        return token.type == '"' || token.type == '\'';
    }

    public void add(GrantEntry ge) {
        grantEntries.addElement(ge);
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

    public static class GrantEntry {
        public String codeBase;
        public final LinkedList<PermissionEntry> permissionEntries = new LinkedList<>();

        public void add(PermissionEntry entry) {
            permissionEntries.add(entry);
        }

        public Enumeration<PermissionEntry> permissionElements() {
            return Collections.enumeration(permissionEntries);
        }

        public void write(PrintWriter out) {
            out.print("grant");
            if (codeBase != null) {
                out.print(" Codebase \"");
                out.print(codeBase);
                out.print("\"");
            }
            out.println(" {");
            for (PermissionEntry pe : permissionEntries) {
                out.print("  permission ");
                out.print(pe.permission);
                if (pe.name != null) {
                    out.print(" \"");
                    out.print(pe.name);
                    out.print("\"");
                }
                if (pe.action != null) {
                    out.print(", \"");
                    out.print(pe.action);
                    out.print("\"");
                }
                out.println(";");
            }
            out.println("};");
        }
    }

    public static class PermissionEntry {
        public String permission;
        public String name;
        public String action;
    }
}
