/*
 * Copyright (c) 1997, 2023, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import javax.security.auth.x500.X500Principal;

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
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

/**
 * Adapted from: https://github.com/openjdk/jdk23u/blob/master/src/java.base/share/classes/sun/security/provider/PolicyParser.java
 */
public class PolicyParser {

    private final Vector<GrantEntry> grantEntries;
    private Map<String, DomainEntry> domainEntries;

    private StreamTokenizer st;
    private int lookahead;
    private boolean expandProp = false;
    private String keyStoreUrlString = null; // unexpanded
    private String keyStoreType = null;
    private String keyStoreProvider = null;
    private String storePassURL = null;

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
            } else if (peek("keystore") && keyStoreUrlString == null) {
                // only one keystore entry per policy file, others will be
                // ignored
                parseKeyStoreEntry();
            } else if (peek("keystorePasswordURL") && storePassURL == null) {
                // only one keystore passwordURL per policy file, others will be
                // ignored
                parseStorePassURL();
            } else if (ge == null && keyStoreUrlString == null && storePassURL == null && peek("domain")) {
                if (domainEntries == null) {
                    domainEntries = new TreeMap<>();
                }
                DomainEntry de = parseDomainEntry();
                String domainName = de.getName();
                if (domainEntries.putIfAbsent(domainName, de) != null) {
                    Object[] source = { domainName };
                    String msg = "duplicate keystore domain name: " + domainName;
                    throw new ParsingException(msg, source);
                }
            } else {
                // error?
            }
            match(";");
        }

        if (keyStoreUrlString == null && storePassURL != null) {
            throw new ParsingException("Keystore Password URL cannot be specified without also specifying keystore");
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
     * Returns the (possibly expanded) keystore location, or null if the
     * expansion fails.
     */
    public String getKeyStoreUrl() {
        try {
            if (keyStoreUrlString != null && keyStoreUrlString.length() != 0) {
                return expand(keyStoreUrlString, true).replace(File.separatorChar, '/');
            }
        } catch (PropertyExpander.ExpandException peee) {
            return null;
        }
        return null;
    }

    public void setKeyStoreUrl(String url) {
        keyStoreUrlString = url;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String type) {
        keyStoreType = type;
    }

    public String getKeyStoreProvider() {
        return keyStoreProvider;
    }

    public void setKeyStoreProvider(String provider) {
        keyStoreProvider = provider;
    }

    public String getStorePassURL() {
        try {
            if (storePassURL != null && storePassURL.length() != 0) {
                return expand(storePassURL, true).replace(File.separatorChar, '/');
            }
        } catch (PropertyExpander.ExpandException peee) {
            return null;
        }
        return null;
    }

    public void setStorePassURL(String storePassURL) {
        this.storePassURL = storePassURL;
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

    public Collection<DomainEntry> getDomainEntries() {
        return domainEntries.values();
    }

    /**
     * write out the policy
     */

    public void write(Writer policy) {
        PrintWriter out = new PrintWriter(new BufferedWriter(policy));

        out.println("/* AUTOMATICALLY GENERATED ON " + (new java.util.Date()) + "*/");
        out.println("/* DO NOT EDIT */");
        out.println();

        // write the (unexpanded) keystore entry as the first entry of the
        // policy file
        if (keyStoreUrlString != null) {
            writeKeyStoreEntry(out);
        }
        if (storePassURL != null) {
            writeStorePassURL(out);
        }

        // write "grant" entries
        for (GrantEntry ge : grantEntries) {
            ge.write(out);
            out.println();
        }
        out.flush();
    }

    /**
     * parses a keystore entry
     */
    private void parseKeyStoreEntry() throws ParsingException, IOException {
        match("keystore");
        keyStoreUrlString = match("quoted string");

        // parse keystore type
        if (!peek(",")) {
            return; // default type
        }
        match(",");

        if (peek("\"")) {
            keyStoreType = match("quoted string");
        } else {
            throw new ParsingException(st.lineno(), "Expected keystore type");
        }

        // parse keystore provider
        if (!peek(",")) {
            return; // provider optional
        }
        match(",");

        if (peek("\"")) {
            keyStoreProvider = match("quoted string");
        } else {
            throw new ParsingException(st.lineno(), "Keystore provider expected");
        }
    }

    private void parseStorePassURL() throws ParsingException, IOException {
        match("keyStorePasswordURL");
        storePassURL = match("quoted string");
    }

    /**
     * writes the (unexpanded) keystore entry
     */
    private void writeKeyStoreEntry(PrintWriter out) {
        out.print("keystore \"");
        out.print(keyStoreUrlString);
        out.print('"');
        if (keyStoreType != null && !keyStoreType.isEmpty()) out.print(", \"" + keyStoreType + "\"");
        if (keyStoreProvider != null && !keyStoreProvider.isEmpty()) out.print(", \"" + keyStoreProvider + "\"");
        out.println(";");
        out.println();
    }

    private void writeStorePassURL(PrintWriter out) {
        out.print("keystorePasswordURL \"");
        out.print(storePassURL);
        out.print('"');
        out.println(";");
        out.println();
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
            } else if (peekAndMatch("SignedBy")) {
                if (e.signedBy != null) throw new ParsingException(st.lineno(), "Multiple SignedBy expressions");
                e.signedBy = match("quoted string");

                // verify syntax of the aliases
                StringTokenizer aliases = new StringTokenizer(e.signedBy, ",", true);
                int actr = 0;
                int cctr = 0;
                while (aliases.hasMoreTokens()) {
                    String alias = aliases.nextToken().trim();
                    if (alias.equals(",")) cctr++;
                    else if (!alias.isEmpty()) actr++;
                }
                if (actr <= cctr) throw new ParsingException(st.lineno(), "SignedBy has an empty alias");

                peekAndMatch(",");
            } else if (peekAndMatch("Principal")) {
                if (principals == null) {
                    principals = new LinkedList<>();
                }

                String principalClass;
                String principalName;

                if (peek("\"")) {
                    // both the principalClass and principalName
                    // will be replaced later
                    principalClass = PrincipalEntry.REPLACE_NAME;
                    principalName = match("principal type");
                } else {
                    // check for principalClass wildcard
                    if (peek("*")) {
                        match("*");
                        principalClass = PrincipalEntry.WILDCARD_CLASS;
                    } else {
                        principalClass = match("principal type");
                    }

                    // check for principalName wildcard
                    if (peek("*")) {
                        match("*");
                        principalName = PrincipalEntry.WILDCARD_NAME;
                    } else {
                        principalName = match("quoted string");
                    }

                    // disallow WILDCARD_CLASS && actual name
                    if (principalClass.equals(PrincipalEntry.WILDCARD_CLASS) && !principalName.equals(PrincipalEntry.WILDCARD_NAME)) {
                        throw new ParsingException(st.lineno(), "Cannot specify Principal with a wildcard class without a wildcard name");
                    }
                }

                try {
                    principalName = expand(principalName);

                    if (principalClass.equals("javax.security.auth.x500.X500Principal")
                        && !principalName.equals(PrincipalEntry.WILDCARD_NAME)) {

                        // 4702543: X500 names with an EmailAddress
                        // were encoded incorrectly. construct a new
                        // X500Principal with correct encoding.

                        X500Principal p = new X500Principal((new X500Principal(principalName)).toString());
                        principalName = p.getName();
                    }

                    principals.add(new PrincipalEntry(principalClass, principalName));
                } catch (PropertyExpander.ExpandException peee) {
                    ignoreEntry = true;
                }
                peekAndMatch(",");

            } else {
                throw new ParsingException(st.lineno(), "Expected codeBase or SignedBy or Principal");
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
            if (e.signedBy != null) e.signedBy = expand(e.signedBy);
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

        if (peekAndMatch("SignedBy")) {
            e.signedBy = expand(match("quoted string"));
        }
        return e;
    }

    /**
     * parse a domain entry
     */
    private DomainEntry parseDomainEntry() throws ParsingException, IOException {
        DomainEntry domainEntry;
        String name;
        Map<String, String> properties = new HashMap<>();

        match("domain");
        name = match("domain name");

        while (!peek("{")) {
            // get the domain properties
            properties = parseProperties("{");
        }
        match("{");
        domainEntry = new DomainEntry(name, properties);

        while (!peek("}")) {

            match("keystore");
            name = match("keystore name");
            // get the keystore properties
            if (!peek("}")) {
                properties = parseProperties(";");
            }
            match(";");
            domainEntry.add(new KeyStoreEntry(name, properties));
        }
        match("}");

        return domainEntry;
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
                } else if (expect.equalsIgnoreCase("domain name")
                    || expect.equalsIgnoreCase("keystore name")
                    || expect.equalsIgnoreCase("property name")) {
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

        public String signedBy;
        public String codeBase;
        public LinkedList<PrincipalEntry> principals;
        public Vector<PermissionEntry> permissionEntries;

        public GrantEntry() {
            principals = new LinkedList<>();
            permissionEntries = new Vector<>();
        }

        public GrantEntry(String signedBy, String codeBase) {
            this.codeBase = codeBase;
            this.signedBy = signedBy;
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
            if (signedBy != null) {
                out.print(" signedBy \"");
                out.print(signedBy);
                out.print('"');
                if (codeBase != null) out.print(", ");
            }
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
            ge.signedBy = this.signedBy;
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
        public String signedBy;

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
                if (that.action != null) return false;
            } else {
                if (!this.action.equals(that.action)) return false;
            }

            if (this.signedBy == null) {
                return that.signedBy == null;
            } else {
                return this.signedBy.equals(that.signedBy);
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
            if (signedBy != null) {
                out.print(", signedBy \"");
                out.print(signedBy);
                out.print('"');
            }
            out.println(";");
        }
    }

    /**
     * Each domain entry in the keystore domain configuration file is
     * represented by a DomainEntry object.
     */
    static class DomainEntry {
        private final String name;
        private final Map<String, String> properties;
        private final Map<String, KeyStoreEntry> entries;

        DomainEntry(String name, Map<String, String> properties) {
            this.name = name;
            this.properties = properties;
            entries = new HashMap<>();
        }

        String getName() {
            return name;
        }

        Map<String, String> getProperties() {
            return properties;
        }

        Collection<KeyStoreEntry> getEntries() {
            return entries.values();
        }

        void add(KeyStoreEntry entry) throws ParsingException {
            String keystoreName = entry.getName();
            if (!entries.containsKey(keystoreName)) {
                entries.put(keystoreName, entry);
            } else {
                Object[] source = { keystoreName };
                String msg = "duplicate keystore name: " + keystoreName;
                throw new ParsingException(msg, source);
            }
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder("\ndomain ").append(name);

            if (properties != null) {
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    s.append("\n        ").append(property.getKey()).append('=').append(property.getValue());
                }
            }
            s.append(" {\n");

            for (KeyStoreEntry entry : entries.values()) {
                s.append(entry).append("\n");
            }
            s.append("}");

            return s.toString();
        }
    }

    /**
     * Each keystore entry in the keystore domain configuration file is
     * represented by a KeyStoreEntry object.
     */

    static class KeyStoreEntry {
        private final String name;
        private final Map<String, String> properties;

        KeyStoreEntry(String name, Map<String, String> properties) {
            this.name = name;
            this.properties = properties;
        }

        String getName() {
            return name;
        }

        Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public String toString() {
            StringBuilder s = new StringBuilder("\n    keystore ").append(name);
            if (properties != null) {
                for (Map.Entry<String, String> property : properties.entrySet()) {
                    s.append("\n        ").append(property.getKey()).append('=').append(property.getValue());
                }
            }
            s.append(";");

            return s.toString();
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
