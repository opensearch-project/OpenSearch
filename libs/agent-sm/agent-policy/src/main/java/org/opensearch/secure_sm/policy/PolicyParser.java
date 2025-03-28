
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.Writer;
import java.security.GeneralSecurityException;
import java.util.Enumeration;
import java.util.Vector;

public class PolicyParser {

    private final Vector<GrantEntry> grantEntries;

    private StreamTokenizer streamTokenizer;
    private int nextToken;
    private boolean expandProp = false;

    private String expand(String value) throws PropertyExpander.ExpandException {
        return expand(value, false);
    }

    private String expand(String value, boolean encodeURL) throws PropertyExpander.ExpandException {
        if (!expandProp) {
            return value;
        } else {
            return PropertyExpander.expand(value, encodeURL);
        }
    }

    /**
     * Creates a PolicyParser object.
     */

    public PolicyParser() {
        grantEntries = new Vector<>();
    }

    public PolicyParser(boolean expandProp) {
        this();
        this.expandProp = expandProp;
    }

    /**
     * Reads a policy configuration into the Policy object using a
     * Reader object.
     *
     * @param policy the policy Reader object.
     *
     * @exception ParsingException if the policy configuration contains
     *          a syntax error.
     *
     * @exception IOException if an error occurs while reading the policy
     *          configuration.
     */

    public void read(Reader policy) throws ParsingException, IOException {
        if (!(policy instanceof BufferedReader)) {
            policy = new BufferedReader(policy);
        }

        /*
         * Configure the stream tokenizer:
         *      Recognize strings between "..."
         *      Don't convert words to lowercase
         *      Recognize both C-style and C++-style comments
         *      Treat end-of-line as white space, not as a token
         */
        streamTokenizer = Tokenizer.configureTokenizer(policy);

        /*
         * The main parsing loop.  The loop is executed once
         * for each entry in the config file.      The entries
         * are delimited by semicolons.   Once we've read in
         * the information for an entry, go ahead and try to
         * add it to the policy vector.
         *
         */
        nextToken = streamTokenizer.nextToken();
        GrantEntry ge = null;
        while (nextToken != StreamTokenizer.TT_EOF) {
            if (peekTokenOnMatch("grant")) {
                ge = parseGrantEntry();
                // could be null if we couldn't expand a property
                if (ge != null) add(ge);
            } else {
                // error?
                // FIX-ME: ERROR-out
            }
            consumeTokenOnMatch(";");
        }
    }

    public void add(GrantEntry ge) {
        grantEntries.addElement(ge);
    }

    public void replace(GrantEntry origGe, GrantEntry newGe) {
        grantEntries.setElementAt(newGe, grantEntries.indexOf(origGe));
    }

    public boolean remove(GrantEntry ge) {
        return grantEntries.removeElement(ge);
    }

    /**
     * Enumerate all the entries in the global policy object.
     * This method is used by policy admin tools. The tools
     * should use the Enumeration methods on the returned object
     * to fetch the elements sequentially.
     */
    public Enumeration<GrantEntry> grantElements() {
        return grantEntries.elements();
    }

    /**
     * write out the policy
     */
    public void write(Writer writer) {
        try (PrintWriter out = new PrintWriter(new BufferedWriter(writer))) {
            out.printf("/* AUTOMATICALLY GENERATED ON %s */%n", new java.util.Date());
            out.println("/* DO NOT EDIT */");
            out.println();

            for (GrantEntry entry : grantEntries) {
                entry.write(out);
                out.println();
            }
        }
    }

    private boolean peekAndMatch(String expect) throws ParsingException, IOException {
        if (peekTokenOnMatch(expect)) {
            consumeTokenOnMatch(expect);
            return true;
        } else {
            return false;
        }
    }

    private String consumeTokenOnMatch(String expected) throws ParsingException, IOException {
        String value = null;

        switch (nextToken) {
            case StreamTokenizer.TT_EOF -> throw new ParsingException("expected [" + expected + "], read [end of file]");
            case StreamTokenizer.TT_NUMBER -> throw new ParsingException(streamTokenizer.lineno(), expected);

            case StreamTokenizer.TT_WORD, '"', '\'' -> {
                value = streamTokenizer.sval;

                if (isExpectedKeyword(expected, value)) {
                    nextToken = streamTokenizer.nextToken();
                    return null;
                }

                if (isTypedToken(expected)) {
                    nextToken = streamTokenizer.nextToken();
                    return value;
                }

                throw new ParsingException(streamTokenizer.lineno(), expected, value);
            }

            case ',', '{', '}', ';', '*', '=' -> {
                if (expected.equals(String.valueOf((char) nextToken))) {
                    nextToken = streamTokenizer.nextToken();
                    return null;
                }
                throw new ParsingException(streamTokenizer.lineno(), expected, String.valueOf((char) nextToken));
            }

            default -> throw new ParsingException(streamTokenizer.lineno(), expected, String.valueOf((char) nextToken));
        }
    }

    private boolean peekTokenOnMatch(String expected) {
        if (nextToken == StreamTokenizer.TT_WORD) {
            return expected.equalsIgnoreCase(streamTokenizer.sval);
        }

        String symbol = switch (nextToken) {
            case ',', '{', '}', '"', '*', ';' -> String.valueOf((char) nextToken);
            default -> null;
        };

        return expected.equalsIgnoreCase(symbol);
    }

    /**
     * parse a Grant entry
     */
    private GrantEntry parseGrantEntry() throws ParsingException, IOException {
        GrantEntry e = new GrantEntry();
        boolean ignoreEntry = false;

        consumeTokenOnMatch("grant");

        while (!peekTokenOnMatch("{")) {

            if (peekAndMatch("Codebase")) {
                if (e.codeBase != null) throw new ParsingException(streamTokenizer.lineno(), "Multiple Codebase expressions");
                e.codeBase = consumeTokenOnMatch("quoted string");
                peekAndMatch(",");
            } else {
                throw new ParsingException(streamTokenizer.lineno(), "Expected codeBase");
            }
        }

        consumeTokenOnMatch("{");

        while (!peekTokenOnMatch("}")) {
            if (peekTokenOnMatch("Permission")) {
                try {
                    PermissionEntry pe = parsePermissionEntry();
                    e.add(pe);
                } catch (PropertyExpander.ExpandException peee) {
                    skipEntry(); // BugId 4219343
                }
                consumeTokenOnMatch(";");
            } else {
                throw new ParsingException(streamTokenizer.lineno(), "Expected permission entry");
            }
        }
        consumeTokenOnMatch("}");

        try {
            if (e.codeBase != null) {
                e.codeBase = expand(e.codeBase, true).replace(File.separatorChar, '/');
            }
        } catch (PropertyExpander.ExpandException peee) {
            return null;
        }

        return (ignoreEntry) ? null : e;
    }

    /**
     * parse a Permission entry
     */
    private PermissionEntry parsePermissionEntry() throws ParsingException, IOException, PropertyExpander.ExpandException {
        PermissionEntry e = new PermissionEntry();

        // Permission
        consumeTokenOnMatch("Permission");
        e.permission = consumeTokenOnMatch("permission type");

        if (peekTokenOnMatch("\"")) {
            // Permission name
            e.name = expand(consumeTokenOnMatch("quoted string"));
        }

        if (!peekTokenOnMatch(",")) {
            return e;
        }
        consumeTokenOnMatch(",");

        if (peekTokenOnMatch("\"")) {
            e.action = expand(consumeTokenOnMatch("quoted string"));
            if (!peekTokenOnMatch(",")) {
                return e;
            }
            consumeTokenOnMatch(",");
        }

        return e;
    }

    private boolean isExpectedKeyword(String expected, String actual) {
        return expected.equalsIgnoreCase(actual);
    }

    private boolean isTypedToken(String expected) {
        return expected.equalsIgnoreCase("permission type") || expected.equalsIgnoreCase("quoted string");
    }

    /**
     * skip all tokens for this entry leaving the delimiter ";"
     * in the stream.
     */
    private void skipEntry() throws ParsingException, IOException {
        while (nextToken != ';') {
            switch (nextToken) {
                case StreamTokenizer.TT_NUMBER:
                    throw new ParsingException(streamTokenizer.lineno(), ";");
                case StreamTokenizer.TT_EOF:
                    throw new ParsingException("Expected read end of file");
                default:
                    nextToken = streamTokenizer.nextToken();
            }
        }
    }

    /**
     * Each grant entry in the policy configuration file is
     * represented by a GrantEntry object.
     *
     * <p>
     * For example, the entry
     *
     * <pre>
     *      grant signedBy "Duke" {
     *          permission java.io.FilePermission "/tmp", "read,write";
     *      };
     *
     * </pre>
     *
     * is represented internally
     *
     * <pre>
     *
     * pe = new PermissionEntry("java.io.FilePermission",
     *         "/tmp", "read,write");
     *
     * ge = new GrantEntry("Duke", null);
     *
     * ge.add(pe);
     *
     * </pre>
     *
     * @author Roland Schemers
     *
     *         version 1.19, 05/21/98
     */

    public static class GrantEntry {

        public String codeBase;
        public Vector<PermissionEntry> permissionEntries;

        public GrantEntry() {
            permissionEntries = new Vector<>();
        }

        public GrantEntry(String codeBase) {
            this.codeBase = codeBase;
            permissionEntries = new Vector<>();
        }

        public void add(PermissionEntry pe) {
            permissionEntries.addElement(pe);
        }

        public boolean remove(PermissionEntry pe) {
            return permissionEntries.removeElement(pe);
        }

        public boolean contains(PermissionEntry pe) {
            return permissionEntries.contains(pe);
        }

        /**
         * Enumerate all the permission entries in this GrantEntry.
         */
        public Enumeration<PermissionEntry> permissionElements() {
            return permissionEntries.elements();
        }

        public void write(PrintWriter out) {
            out.print("grant");
            if (codeBase != null) {
                out.print(" codeBase \"");
                out.print(codeBase);
                out.print('"');
            }
            out.println(" {");
            for (PermissionEntry pe : permissionEntries) {
                out.write("  ");
                pe.write(out);
            }
            out.println("};");
        }

        public Object clone() {
            GrantEntry ge = new GrantEntry();
            ge.codeBase = this.codeBase;
            ge.permissionEntries = new Vector<>(this.permissionEntries);
            return ge;
        }
    }

    /**
     * Each permission entry in the policy configuration file is
     * represented by a
     * PermissionEntry object.
     *
     * <p>
     * For example, the entry
     *
     * <pre>
     *          permission java.io.FilePermission "/tmp", "read,write";
     * </pre>
     *
     * is represented internally
     *
     * <pre>
     *
     * pe = new PermissionEntry("java.io.FilePermission",
     *         "/tmp", "read,write");
     * </pre>
     *
     * @author Roland Schemers
     *
     *         version 1.19, 05/21/98
     */

    public static class PermissionEntry {

        public String permission;
        public String name;
        public String action;

        public PermissionEntry() {}

        public PermissionEntry(String permission, String name, String action) {
            this.permission = permission;
            this.name = name;
            this.action = action;
        }

        /**
         * Calculates a hash code value for the object. Objects
         * which are equal will also have the same hashcode.
         */
        @Override
        public int hashCode() {
            int retval = permission.hashCode();
            if (name != null) retval ^= name.hashCode();
            if (action != null) retval ^= action.hashCode();
            return retval;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;

            if (!(obj instanceof PermissionEntry that)) return false;

            if (this.permission == null) {
                if (that.permission != null) return false;
            } else {
                if (!this.permission.equals(that.permission)) return false;
            }

            if (this.name == null) {
                if (that.name != null) return false;
            } else {
                if (!this.name.equals(that.name)) return false;
            }

            if (this.action == null) {
                return that.action == null;
            } else {
                return this.action.equals(that.action);
            }

        }

        public void write(PrintWriter out) {
            out.print("permission ");
            out.print(permission);
            if (name != null) {
                out.print(" \"");

                // ATTENTION: regex with double escaping,
                // the normal forms look like:
                // $name =~ s/\\/\\\\/g; and
                // $name =~ s/\"/\\\"/g;
                // and then in a java string, it's escaped again

                out.print(name.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\\\""));
                out.print('"');
            }
            if (action != null) {
                out.print(", \"");
                out.print(action);
                out.print('"');
            }

            out.println(";");
        }
    }

    public static class ParsingException extends GeneralSecurityException {

        @java.io.Serial
        private static final long serialVersionUID = -4330692689482574072L;

        @SuppressWarnings("serial") // Not statically typed as Serializable
        private Object[] source;

        /**
         * Constructs a ParsingException with the specified
         * detail message. A detail message is a String that describes
         * this particular exception, which may, for example, specify which
         * algorithm is not available.
         *
         * @param msg the detail message.
         */
        public ParsingException(String msg) {
            super(msg);
        }

        public ParsingException(String msg, Object[] source) {
            super(msg);
            this.source = source;
        }

        public ParsingException(int line, String msg) {
            super("line " + line + ": " + msg);
            source = new Object[] { line, msg };
        }

        public ParsingException(int line, String expect, String actual) {
            super("line " + line + ": expected [" + expect + "], found [" + actual + "]");
            source = new Object[] { line, expect, actual };
        }
    }
}
