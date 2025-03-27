
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
import java.security.Principal;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Vector;

/**
 * Adapted from: https://github.com/openjdk/jdk23u/blob/master/src/java.base/share/classes/sun/security/provider/PolicyParser.java
 */
public class PolicyParser {

    private final Vector<GrantEntry> grantEntries;

    private StreamTokenizer st;
    private int lookahead;
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
        st = new StreamTokenizer(policy);

        st.resetSyntax();
        st.wordChars('a', 'z');
        st.wordChars('A', 'Z');
        st.wordChars('.', '.');
        st.wordChars('0', '9');
        st.wordChars('_', '_');
        st.wordChars('$', '$');
        st.wordChars(128 + 32, 255);
        st.whitespaceChars(0, ' ');
        st.commentChar('/');
        st.quoteChar('\'');
        st.quoteChar('"');
        st.lowerCaseMode(false);
        st.ordinaryChar('/');
        st.slashSlashComments(true);
        st.slashStarComments(true);

        /*
         * The main parsing loop.  The loop is executed once
         * for each entry in the config file.      The entries
         * are delimited by semicolons.   Once we've read in
         * the information for an entry, go ahead and try to
         * add it to the policy vector.
         *
         */

        lookahead = st.nextToken();
        GrantEntry ge = null;
        while (lookahead != StreamTokenizer.TT_EOF) {
            if (peek("grant")) {
                ge = parseGrantEntry();
                // could be null if we couldn't expand a property
                if (ge != null) add(ge);
            } else {
                // error?
                // FIX-ME: ERROR-out
            }
            match(";");
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
     * This method is used by policy admin tools.   The tools
     * should use the Enumeration methods on the returned object
     * to fetch the elements sequentially.
     */
    public Enumeration<GrantEntry> grantElements() {
        return grantEntries.elements();
    }

    /**
     * write out the policy
     */

    public void write(Writer policy) {
        PrintWriter out = new PrintWriter(new BufferedWriter(policy));

        out.println("/* AUTOMATICALLY GENERATED ON " + (new java.util.Date()) + "*/");
        out.println("/* DO NOT EDIT */");
        out.println();

        // write "grant" entries
        for (GrantEntry ge : grantEntries) {
            ge.write(out);
            out.println();
        }
        out.flush();
    }

    /**
     * parse a Grant entry
     */
    private GrantEntry parseGrantEntry() throws ParsingException, IOException {
        GrantEntry e = new GrantEntry();
        LinkedList<PrincipalEntry> principals = null;
        boolean ignoreEntry = false;

        match("grant");

        while (!peek("{")) {

            if (peekAndMatch("Codebase")) {
                if (e.codeBase != null) throw new ParsingException(st.lineno(), "Multiple Codebase expressions");
                e.codeBase = match("quoted string");
                peekAndMatch(",");
            } else {
                throw new ParsingException(st.lineno(), "Expected codeBase");
            }
        }

        if (principals != null) e.principals = principals;
        match("{");

        while (!peek("}")) {
            if (peek("Permission")) {
                try {
                    PermissionEntry pe = parsePermissionEntry();
                    e.add(pe);
                } catch (PropertyExpander.ExpandException peee) {
                    skipEntry();  // BugId 4219343
                }
                match(";");
            } else {
                throw new ParsingException(st.lineno(), "Expected permission entry");
            }
        }
        match("}");

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
        match("Permission");
        e.permission = match("permission type");

        if (peek("\"")) {
            // Permission name
            e.name = expand(match("quoted string"));
        }

        if (!peek(",")) {
            return e;
        }
        match(",");

        if (peek("\"")) {
            e.action = expand(match("quoted string"));
            if (!peek(",")) {
                return e;
            }
            match(",");
        }

        return e;
    }

    /*
     * Return a collection of domain properties or keystore properties.
     */
    private Map<String, String> parseProperties(String terminator) throws ParsingException, IOException {

        Map<String, String> properties = new HashMap<>();
        String key;
        String value;
        while (!peek(terminator)) {
            key = match("property name");
            match("=");

            try {
                value = expand(match("quoted string"));
            } catch (PropertyExpander.ExpandException peee) {
                throw new IOException(peee.getLocalizedMessage());
            }
            properties.put(key.toLowerCase(Locale.ENGLISH), value);
        }

        return properties;
    }

    private boolean peekAndMatch(String expect) throws ParsingException, IOException {
        if (peek(expect)) {
            match(expect);
            return true;
        } else {
            return false;
        }
    }

    private boolean peek(String expect) {
        boolean found = false;

        switch (lookahead) {

            case StreamTokenizer.TT_WORD:
                if (expect.equalsIgnoreCase(st.sval)) found = true;
                break;
            case ',':
                if (expect.equalsIgnoreCase(",")) found = true;
                break;
            case '{':
                if (expect.equalsIgnoreCase("{")) found = true;
                break;
            case '}':
                if (expect.equalsIgnoreCase("}")) found = true;
                break;
            case '"':
                if (expect.equalsIgnoreCase("\"")) found = true;
                break;
            case '*':
                if (expect.equalsIgnoreCase("*")) found = true;
                break;
            case ';':
                if (expect.equalsIgnoreCase(";")) found = true;
                break;
            default:

        }
        return found;
    }

    private String match(String expect) throws ParsingException, IOException {
        String value = null;

        switch (lookahead) {
            case StreamTokenizer.TT_NUMBER:
                throw new ParsingException(st.lineno(), expect);
            case StreamTokenizer.TT_EOF:
                Object[] source = { expect };
                String msg = "expected [" + expect + "], read [end of file]";
                throw new ParsingException(msg, source);
            case StreamTokenizer.TT_WORD:
                if (expect.equalsIgnoreCase(st.sval)) {
                    lookahead = st.nextToken();
                } else if (expect.equalsIgnoreCase("permission type")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else if (expect.equalsIgnoreCase("principal type")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else if (expect.equalsIgnoreCase("property name")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else {
                    throw new ParsingException(st.lineno(), expect, st.sval);
                }
                break;
            case '"':
                if (expect.equalsIgnoreCase("quoted string")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else if (expect.equalsIgnoreCase("permission type")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else if (expect.equalsIgnoreCase("principal type")) {
                    value = st.sval;
                    lookahead = st.nextToken();
                } else {
                    throw new ParsingException(st.lineno(), expect, st.sval);
                }
                break;
            case ',':
                if (expect.equalsIgnoreCase(",")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, ",");
                break;
            case '{':
                if (expect.equalsIgnoreCase("{")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, "{");
                break;
            case '}':
                if (expect.equalsIgnoreCase("}")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, "}");
                break;
            case ';':
                if (expect.equalsIgnoreCase(";")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, ";");
                break;
            case '*':
                if (expect.equalsIgnoreCase("*")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, "*");
                break;
            case '=':
                if (expect.equalsIgnoreCase("=")) lookahead = st.nextToken();
                else throw new ParsingException(st.lineno(), expect, "=");
                break;
            default:
                throw new ParsingException(st.lineno(), expect, String.valueOf((char) lookahead));
        }
        return value;
    }

    /**
     * skip all tokens for this entry leaving the delimiter ";"
     * in the stream.
     */
    private void skipEntry() throws ParsingException, IOException {
        while (lookahead != ';') {
            switch (lookahead) {
                case StreamTokenizer.TT_NUMBER:
                    throw new ParsingException(st.lineno(), ";");
                case StreamTokenizer.TT_EOF:
                    throw new ParsingException("Expected read end of file");
                default:
                    lookahead = st.nextToken();
            }
        }
    }

    /**
     * Each grant entry in the policy configuration file is
     * represented by a GrantEntry object.
     *
     * <p>
     * For example, the entry
     * <pre>
     *      grant signedBy "Duke" {
     *          permission java.io.FilePermission "/tmp", "read,write";
     *      };
     *
     * </pre>
     * is represented internally
     * <pre>
     *
     * pe = new PermissionEntry("java.io.FilePermission",
     *                           "/tmp", "read,write");
     *
     * ge = new GrantEntry("Duke", null);
     *
     * ge.add(pe);
     *
     * </pre>
     *
     * @author Roland Schemers
     *
     * version 1.19, 05/21/98
     */

    public static class GrantEntry {

        public String codeBase;
        public LinkedList<PrincipalEntry> principals;
        public Vector<PermissionEntry> permissionEntries;

        public GrantEntry() {
            principals = new LinkedList<>();
            permissionEntries = new Vector<>();
        }

        public GrantEntry(String codeBase) {
            this.codeBase = codeBase;
            principals = new LinkedList<>();
            permissionEntries = new Vector<>();
        }

        public void add(PermissionEntry pe) {
            permissionEntries.addElement(pe);
        }

        public boolean remove(PrincipalEntry pe) {
            return principals.remove(pe);
        }

        public boolean remove(PermissionEntry pe) {
            return permissionEntries.removeElement(pe);
        }

        public boolean contains(PrincipalEntry pe) {
            return principals.contains(pe);
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
                if (principals != null && principals.size() > 0) out.print(",\n");
            }
            if (principals != null && principals.size() > 0) {
                Iterator<PrincipalEntry> pli = principals.iterator();
                while (pli.hasNext()) {
                    out.print("      ");
                    PrincipalEntry pe = pli.next();
                    pe.write(out);
                    if (pli.hasNext()) out.print(",\n");
                }
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
            ge.principals = new LinkedList<>(this.principals);
            ge.permissionEntries = new Vector<>(this.permissionEntries);
            return ge;
        }
    }

    /**
     * Principal info (class and name) in a grant entry
     */
    public static class PrincipalEntry implements Principal {

        public static final String WILDCARD_CLASS = "WILDCARD_PRINCIPAL_CLASS";
        public static final String WILDCARD_NAME = "WILDCARD_PRINCIPAL_NAME";
        public static final String REPLACE_NAME = "PolicyParser.REPLACE_NAME";

        String principalClass;
        String principalName;

        /**
         * A PrincipalEntry consists of the Principal class and Principal name.
         *
         * @param principalClass the Principal class
         * @param principalName the Principal name
         * @throws NullPointerException if principalClass or principalName
         *                              are null
         */
        public PrincipalEntry(String principalClass, String principalName) {
            if (principalClass == null || principalName == null) throw new NullPointerException("principalClass or principalName is null");
            this.principalClass = principalClass;
            this.principalName = principalName;
        }

        boolean isWildcardName() {
            return principalName.equals(WILDCARD_NAME);
        }

        boolean isWildcardClass() {
            return principalClass.equals(WILDCARD_CLASS);
        }

        boolean isReplaceName() {
            return principalClass.equals(REPLACE_NAME);
        }

        public String getPrincipalClass() {
            return principalClass;
        }

        public String getPrincipalName() {
            return principalName;
        }

        public String getDisplayClass() {
            if (isWildcardClass()) {
                return "*";
            } else if (isReplaceName()) {
                return "";
            } else return principalClass;
        }

        public String getDisplayName() {
            return getDisplayName(false);
        }

        public String getDisplayName(boolean addQuote) {
            if (isWildcardName()) {
                return "*";
            } else {
                if (addQuote) return "\"" + principalName + "\"";
                else return principalName;
            }
        }

        @Override
        public String getName() {
            return principalName;
        }

        @Override
        public String toString() {
            if (!isReplaceName()) {
                return getDisplayClass() + "/" + getDisplayName();
            } else {
                return getDisplayName();
            }
        }

        /**
         * Test for equality between the specified object and this object.
         * Two PrincipalEntries are equal if their class and name values
         * are equal.
         *
         * @param obj the object to test for equality with this object
         * @return true if the objects are equal, false otherwise
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;

            if (!(obj instanceof PrincipalEntry that)) return false;

            return (principalClass.equals(that.principalClass) && principalName.equals(that.principalName));
        }

        /**
         * Return a hashcode for this PrincipalEntry.
         *
         * @return a hashcode for this PrincipalEntry
         */
        @Override
        public int hashCode() {
            return principalClass.hashCode();
        }

        public void write(PrintWriter out) {
            out.print("principal " + getDisplayClass() + " " + getDisplayName(true));
        }
    }

    /**
     * Each permission entry in the policy configuration file is
     * represented by a
     * PermissionEntry object.
     *
     * <p>
     * For example, the entry
     * <pre>
     *          permission java.io.FilePermission "/tmp", "read,write";
     * </pre>
     * is represented internally
     * <pre>
     *
     * pe = new PermissionEntry("java.io.FilePermission",
     *                           "/tmp", "read,write");
     * </pre>
     *
     * @author Roland Schemers
     *
     * version 1.19, 05/21/98
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
         * Calculates a hash code value for the object.  Objects
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
