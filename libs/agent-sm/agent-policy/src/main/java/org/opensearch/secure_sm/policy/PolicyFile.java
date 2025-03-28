/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.URI;
import java.net.URL;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.Security;
import java.security.UnresolvedPermission;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Adapted from:
 * https://github.com/openjdk/jdk23u/blob/master/src/java.base/share/classes/sun/security/provider/PolicyFile.java
 */
@SuppressWarnings("removal")
public class PolicyFile extends java.security.Policy {
    private static final String POLICY = "java.security.policy";
    private static final String POLICY_URL = "policy.url.";
    public static final SocketPermission LOCAL_LISTEN_PERMISSION = new SocketPermission("localhost:0", "listen");

    private static final int DEFAULT_CACHE_SIZE = 1;

    // contains the policy grant entries, PD cache, and alias mapping
    // can be updated if refresh() is called
    private volatile PolicyInfo policyInfo;

    private boolean expandProperties = true;
    private boolean allowSystemProperties = true;
    private boolean notUtf8 = false;
    private URL url;

    // for use with the reflection API
    private static final Class<?>[] PARAMS0 = {};
    private static final Class<?>[] PARAMS1 = { String.class };
    private static final Class<?>[] PARAMS2 = { String.class, String.class };

    /**
     * When a policy file has a syntax error, the exception code may generate
     * another permission check and this can cause the policy file to be parsed
     * repeatedly, leading to a StackOverflowError or ClassCircularityError.
     * To avoid this, this set is populated with policy files that have been
     * previously parsed and have syntax errors, so that they can be
     * subsequently ignored.
     */
    private static Set<URL> badPolicyURLs = Collections.newSetFromMap(new ConcurrentHashMap<URL, Boolean>());

    /**
     * Initializes the Policy object and reads the default policy
     * configuration file(s) into the Policy object.
     */
    public PolicyFile() {
        init((URL) null);
    }

    /**
     * Initializes the Policy object and reads the default policy
     * from the specified URL only.
     */
    public PolicyFile(URL url) {
        this.url = url;
        init(url);
    }

    /**
     * Initializes the Policy object and reads the default policy
     * configuration file(s) into the Policy object.
     *
     * See the class description for details on the algorithm used to
     * initialize the Policy object.
     */
    private void init(URL url) {
        int numCaches = DEFAULT_CACHE_SIZE;
        PolicyInfo newInfo = new PolicyInfo(numCaches);
        initPolicyFile(newInfo, url);
        policyInfo = newInfo;
    }

    private void initPolicyFile(final PolicyInfo newInfo, final URL url) {
        if (url != null) {

            /**
             * If the caller specified a URL via Policy.getInstance,
             * we only read from default.policy and that URL.
             */

            if (init(url, newInfo) == false) {
                // use static policy if all else fails
                initStaticPolicy(newInfo);
            }

        } else {

            /**
             * Caller did not specify URL via Policy.getInstance.
             * Read from URLs listed in the java.security properties file.
             */

            boolean loaded_one = initPolicyFile(POLICY, POLICY_URL, newInfo);
            // To maintain strict backward compatibility
            // we load the static policy only if POLICY load failed
            if (!loaded_one) {
                // use static policy if all else fails
                initStaticPolicy(newInfo);
            }
        }
    }

    private boolean initPolicyFile(final String propname, final String urlname, final PolicyInfo newInfo) {
        boolean loaded_policy = false;

        if (allowSystemProperties) {
            String extra_policy = System.getProperty(propname);
            if (extra_policy != null) {
                boolean overrideAll = false;
                if (extra_policy.startsWith("=")) {
                    overrideAll = true;
                    extra_policy = extra_policy.substring(1);
                }
                try {
                    extra_policy = PropertyExpander.expand(extra_policy);
                    URL policyURL;

                    File policyFile = new File(extra_policy);
                    if (policyFile.exists()) {
                        policyURL = ParseUtil.fileToEncodedURL(new File(policyFile.getCanonicalPath()));
                    } else {
                        policyURL = newURL(extra_policy);
                    }
                    if (init(policyURL, newInfo)) {
                        loaded_policy = true;
                    }
                } catch (Exception e) {}
                if (overrideAll) {
                    return Boolean.valueOf(loaded_policy);
                }
            }
        }

        int n = 1;
        String policy_uri;

        while ((policy_uri = Security.getProperty(urlname + n)) != null) {
            try {
                URL policy_url = null;
                String expanded_uri = PropertyExpander.expand(policy_uri).replace(File.separatorChar, '/');

                if (policy_uri.startsWith("file:${java.home}/") || policy_uri.startsWith("file:${user.home}/")) {

                    // this special case accommodates
                    // the situation java.home/user.home
                    // expand to a single slash, resulting in
                    // a file://foo URI
                    policy_url = new File(expanded_uri.substring(5)).toURI().toURL();
                } else {
                    policy_url = new URI(expanded_uri).toURL();
                }

                if (init(policy_url, newInfo)) {
                    loaded_policy = true;
                }
            } catch (Exception e) {
                // ignore that policy
            }
            n++;
        }
        return Boolean.valueOf(loaded_policy);
    }

    /**
     * Reads a policy configuration into the Policy object using a
     * Reader object.
     */
    private boolean init(URL policy, PolicyInfo newInfo) {

        // skip parsing policy file if it has been previously parsed and
        // has syntax errors
        if (badPolicyURLs.contains(policy)) {
            return false;
        }

        try (InputStreamReader isr = getInputStreamReader(getInputStream(policy))) {

            PolicyParser pp = new PolicyParser(expandProperties);
            pp.read(isr);

            Enumeration<PolicyParser.GrantEntry> enum_ = pp.grantElements();
            while (enum_.hasMoreElements()) {
                PolicyParser.GrantEntry ge = enum_.nextElement();
                addGrantEntry(ge, newInfo);
            }
            return true;
        } catch (PolicyParser.ParsingException pe) {
            // record bad policy file to avoid later reparsing it
            badPolicyURLs.add(policy);
            pe.printStackTrace(System.err);
        } catch (Exception e) {}

        return false;
    }

    public static InputStream getInputStream(URL url) throws IOException {
        if ("file".equals(url.getProtocol())) {
            String path = url.getFile().replace('/', File.separatorChar);
            path = ParseUtil.decode(path);
            return new FileInputStream(path);
        } else {
            return url.openStream();
        }
    }

    private InputStreamReader getInputStreamReader(InputStream is) {
        /*
         * Read in policy using UTF-8 by default.
         *
         * Check non-standard system property to see if the default encoding
         * should be used instead.
         */
        return (notUtf8) ? new InputStreamReader(is) : new InputStreamReader(is, UTF_8);
    }

    private void initStaticPolicy(final PolicyInfo newInfo) {
        PolicyEntry pe = new PolicyEntry(new CodeSource(null, (Certificate[]) null));
        pe.add(LOCAL_LISTEN_PERMISSION);

        // No need to sync because no one has access to newInfo yet
        newInfo.policyEntries.add(pe);
    }

    /**
     * Given a GrantEntry, create a codeSource.
     *
     * @return null if signedBy alias is not recognized
     */
    private CodeSource getCodeSource(PolicyParser.GrantEntry ge, PolicyInfo newInfo) throws java.net.MalformedURLException {
        Certificate[] certs = null;
        URL location;

        if (ge.codeBase != null) location = newURL(ge.codeBase);
        else location = null;

        return (canonicalizeCodebase(new CodeSource(location, certs)));
    }

    /**
     * Add one policy entry to the list.
     */
    private void addGrantEntry(PolicyParser.GrantEntry ge, PolicyInfo newInfo) {

        try {
            CodeSource codesource = getCodeSource(ge, newInfo);
            // skip if signedBy alias was unknown...
            if (codesource == null) return;

            PolicyEntry entry = new PolicyEntry(codesource);
            Enumeration<PolicyParser.PermissionEntry> enum_ = ge.permissionElements();
            while (enum_.hasMoreElements()) {
                PolicyParser.PermissionEntry pe = enum_.nextElement();

                try {
                    // perform ${{ ... }} expansions within permission name
                    expandPermissionName(pe);

                    Permission perm = getInstance(pe.permission, pe.name, pe.action);

                    entry.add(perm);
                } catch (ClassNotFoundException cnfe) {
                    // maybe FIX ME.
                    Certificate[] certs = null;
                    Permission perm = new UnresolvedPermission(pe.permission, pe.name, pe.action, certs);
                    entry.add(perm);

                } catch (java.lang.reflect.InvocationTargetException ite) {
                    ite.printStackTrace(System.err);
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            }

            // No need to sync because no one has access to newInfo yet
            newInfo.policyEntries.add(entry);
        } catch (

        Exception e) {
            e.printStackTrace(System.err);
        }
    }

    /**
     * Returns a new Permission object of the given Type. The Permission is
     * created by getting the
     * Class object using the <code>Class.forName</code> method, and using
     * the reflection API to invoke the (String name, String actions)
     * constructor on the
     * object.
     *
     * @param type    the type of Permission being created.
     * @param name    the name of the Permission being created.
     * @param actions the actions of the Permission being created.
     *
     * @exception ClassNotFoundException    if the particular Permission
     *                                      class could not be found.
     *
     * @exception IllegalAccessException    if the class or initializer is
     *                                      not accessible.
     *
     * @exception InstantiationException    if getInstance tries to
     *                                      instantiate an abstract class or an
     *                                      interface, or if the
     *                                      instantiation fails for some other
     *                                      reason.
     *
     * @exception NoSuchMethodException     if the (String, String) constructor
     *                                      is not found.
     *
     * @exception InvocationTargetException if the underlying Permission
     *                                      constructor throws an exception.
     *
     */

    private static final Permission getInstance(String type, String name, String actions) throws ClassNotFoundException,
        InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Class<?> pc = Class.forName(type, false, null);
        Permission answer = getKnownPermission(pc, name, actions);
        if (answer != null) {
            return answer;
        }
        if (!Permission.class.isAssignableFrom(pc)) {
            // not the right subtype
            throw new ClassCastException(type + " is not a Permission");
        }

        if (name == null && actions == null) {
            try {
                Constructor<?> c = pc.getConstructor(PARAMS0);
                return (Permission) c.newInstance(new Object[] {});
            } catch (NoSuchMethodException ne) {
                try {
                    Constructor<?> c = pc.getConstructor(PARAMS1);
                    return (Permission) c.newInstance(new Object[] { name });
                } catch (NoSuchMethodException ne1) {
                    Constructor<?> c = pc.getConstructor(PARAMS2);
                    return (Permission) c.newInstance(new Object[] { name, actions });
                }
            }
        } else {
            if (name != null && actions == null) {
                try {
                    Constructor<?> c = pc.getConstructor(PARAMS1);
                    return (Permission) c.newInstance(new Object[] { name });
                } catch (NoSuchMethodException ne) {
                    Constructor<?> c = pc.getConstructor(PARAMS2);
                    return (Permission) c.newInstance(new Object[] { name, actions });
                }
            } else {
                Constructor<?> c = pc.getConstructor(PARAMS2);
                return (Permission) c.newInstance(new Object[] { name, actions });
            }
        }
    }

    /**
     * Creates one of the well-known permissions in the java.base module
     * directly instead of via reflection. Keep list short to not penalize
     * permissions from other modules.
     */
    private static Permission getKnownPermission(Class<?> claz, String name, String actions) {
        if (claz.equals(FilePermission.class)) {
            return new FilePermission(name, actions);
        } else if (claz.equals(SocketPermission.class)) {
            return new SocketPermission(name, actions);
        } else if (claz.equals(NetPermission.class)) {
            return new NetPermission(name, actions);
        } else {
            return null;
        }
    }

    /**
     * Refreshes the policy object by re-reading all the policy files.
     */
    @Override
    public void refresh() {
        init(url);
    }

    /**
     * Evaluates the global policy for the permissions granted to
     * the ProtectionDomain and tests whether the permission is
     * granted.
     *
     * @param pd the ProtectionDomain to test
     * @param p  the Permission object to be tested for implication.
     *
     * @return true if "permission" is a proper subset of a permission
     *         granted to this ProtectionDomain.
     *
     * @see java.security.ProtectionDomain
     */
    @Override
    public boolean implies(ProtectionDomain pd, Permission p) {
        PermissionCollection pc = getPermissions(pd);
        if (pc == null) {
            return false;
        }

        // cache mapping of protection domain to its PermissionCollection
        return pc.implies(p);
    }

    /**
     * Examines this <code>Policy</code> and returns the permissions granted
     * to the specified <code>ProtectionDomain</code>. This includes
     * the permissions currently associated with the domain as well
     * as the policy permissions granted to the domain's
     * CodeSource, ClassLoader, and Principals.
     *
     * <p>
     * Note that this <code>Policy</code> implementation has
     * special handling for PrivateCredentialPermissions.
     * When this method encounters a <code>PrivateCredentialPermission</code>
     * which specifies "self" as the <code>Principal</code> class and name,
     * it does not add that <code>Permission</code> to the returned
     * <code>PermissionCollection</code>. Instead, it builds
     * a new <code>PrivateCredentialPermission</code>
     * for each <code>Principal</code> associated with the provided
     * <code>Subject</code>. Each new <code>PrivateCredentialPermission</code>
     * contains the same Credential class as specified in the
     * originally granted permission, as well as the Class and name
     * for the respective <code>Principal</code>.
     *
     * @param domain the Permissions granted to this
     *               <code>ProtectionDomain</code> are returned.
     *
     * @return the Permissions granted to the provided
     *         <code>ProtectionDomain</code>.
     */
    @Override
    public PermissionCollection getPermissions(ProtectionDomain domain) {
        Permissions perms = new Permissions();

        if (domain == null) return perms;

        // first get policy perms
        getPermissions(perms, domain);

        // add static perms
        // - adding static perms after policy perms is necessary
        // to avoid a regression for 4301064
        PermissionCollection pc = domain.getPermissions();
        if (pc != null) {
            synchronized (pc) {
                Enumeration<Permission> e = pc.elements();
                while (e.hasMoreElements()) {
                    perms.add(e.nextElement());
                }
            }
        }

        return perms;
    }

    /**
     * Examines this Policy and creates a PermissionCollection object with
     * the set of permissions for the specified CodeSource.
     *
     * @param codesource the CodeSource associated with the caller.
     *                   This encapsulates the original location of the code (where
     *                   the code
     *                   came from) and the public key(s) of its signer.
     *
     * @return the set of permissions according to the policy.
     */
    @Override
    public PermissionCollection getPermissions(CodeSource codesource) {
        return getPermissions(new Permissions(), codesource);
    }

    /**
     * Examines the global policy and returns the provided Permissions
     * object with additional permissions granted to the specified
     * ProtectionDomain.
     *
     * @param perms the Permissions to populate
     * @param pd    the ProtectionDomain associated with the caller.
     *
     * @return the set of Permissions according to the policy.
     */
    private PermissionCollection getPermissions(Permissions perms, ProtectionDomain pd) {
        final CodeSource cs = pd.getCodeSource();
        if (cs == null) return perms;

        CodeSource canonCodeSource = canonicalizeCodebase(cs);
        return getPermissions(perms, canonCodeSource);
    }

    /**
     * Examines the global policy and returns the provided Permissions
     * object with additional permissions granted to the specified
     * CodeSource.
     *
     * @param perms the permissions to populate
     * @param cs    the codesource associated with the caller.
     *              This encapsulates the original location of the code (where the
     *              code
     *              came from) and the public key(s) of its signer.
     *
     * @return the set of permissions according to the policy.
     */
    private PermissionCollection getPermissions(Permissions perms, final CodeSource cs) {
        if (cs == null) {
            return perms;
        }

        CodeSource canonCodeSource = canonicalizeCodebase(cs);

        for (PolicyEntry entry : policyInfo.policyEntries) {
            addPermissions(perms, cs, entry);
        }

        return perms;
    }

    private void addPermissions(Permissions perms, final CodeSource cs, final PolicyEntry entry) {

        // check to see if the CodeSource implies
        Boolean imp = entry.getCodeSource().implies(cs);
        if (!imp.booleanValue()) {
            // CodeSource does not imply - return and try next policy entry
            return;
        }

        addPerms(perms, entry);
    }

    private void addPerms(Permissions perms, PolicyEntry entry) {
        for (int i = 0; i < entry.permissions.size(); i++) {
            Permission p = entry.permissions.get(i);

            perms.add(p);

        }
    }

    private CodeSource canonicalizeCodebase(CodeSource cs) {
        String path = null;

        CodeSource canonCs = cs;
        URL u = cs.getLocation();
        if (u != null) {
            if (u.getProtocol().equals("jar")) {
                // unwrap url embedded inside jar url
                String spec = u.getFile();
                int separator = spec.indexOf("!/");
                if (separator != -1) {
                    try {
                        u = newURL(spec.substring(0, separator));
                    } catch (MalformedURLException e) {
                        // Fail silently. In this case, url stays what
                        // it was above
                    }
                }
            }
            if (u.getProtocol().equals("file")) {
                boolean isLocalFile = false;
                String host = u.getHost();
                isLocalFile = (host == null || host.isEmpty() || host.equals("~") || host.equalsIgnoreCase("localhost"));

                if (isLocalFile) {
                    path = u.getFile().replace('/', File.separatorChar);
                    path = ParseUtil.decode(path);
                }
            }
        }

        if (path != null) {
            try {
                URL csUrl = null;
                path = canonPath(path);
                csUrl = ParseUtil.fileToEncodedURL(new File(path));
                canonCs = new CodeSource(csUrl, cs.getCertificates());
            } catch (IOException ioe) {
                // leave codesource as it is...
                // FIX ME: log an exception?
            }
        }

        return canonCs;
    }

    // Wrapper to return a canonical path that avoids calling getCanonicalPath()
    // with paths that are intended to match all entries in the directory
    private static String canonPath(String path) throws IOException {
        if (path.endsWith("*")) {
            path = path.substring(0, path.length() - 1) + "-";
            path = new File(path).getCanonicalPath();
            return path.substring(0, path.length() - 1) + "*";
        } else {
            return new File(path).getCanonicalPath();
        }
    }

    private void expandPermissionName(PolicyParser.PermissionEntry pe) throws Exception {
        // short cut the common case
        if (pe.name == null || pe.name.indexOf("${{", 0) == -1) {
            return;
        }

        int startIndex = 0;
        int b, e;
        StringBuilder sb = new StringBuilder();
        while ((b = pe.name.indexOf("${{", startIndex)) != -1) {
            e = pe.name.indexOf("}}", b);
            if (e < 1) {
                break;
            }
            sb.append(pe.name.substring(startIndex, b));

            // get the value in ${{...}}
            String value = pe.name.substring(b + 3, e);

            // parse up to the first ':'
            int colonIndex;
            String prefix = value;
            String suffix;
            if ((colonIndex = value.indexOf(':')) != -1) {
                prefix = value.substring(0, colonIndex);
            }
        }

        // copy the rest of the value
        sb.append(pe.name.substring(startIndex));

        pe.name = sb.toString();
    }

    /**
     * Each entry in the policy configuration file is represented by a
     * PolicyEntry object.
     * <p>
     *
     * A PolicyEntry is a (CodeSource,Permission) pair. The
     * CodeSource contains the (URL, PublicKey) that together identify
     * where the Java bytecodes come from and who (if anyone) signed
     * them. The URL could refer to localhost. The URL could also be
     * null, meaning that this policy entry is given to all comers, as
     * long as they match the signer field. The signer could be null,
     * meaning the code is not signed.
     * <p>
     *
     * The Permission contains the (Type, Name, Action) triplet.
     * <p>
     *
     * For now, the Policy object retrieves the public key from the
     * X.509 certificate on disk that corresponds to the signedBy
     * alias specified in the Policy config file. For reasons of
     * efficiency, the Policy object keeps a hashtable of certs already
     * read in. This could be replaced by a secure internal key
     * store.
     *
     * <p>
     * For example, the entry
     *
     * <pre>
     *          permission java.io.File "/tmp", "read,write",
     *          signedBy "Duke";
     * </pre>
     *
     * is represented internally
     *
     * <pre>
     *
     * FilePermission f = new FilePermission("/tmp", "read,write");
     * PublicKey p = publickeys.get("Duke");
     * URL u = InetAddress.getLocalHost();
     * CodeBase c = new CodeBase(p, u);
     * pe = new PolicyEntry(f, c);
     * </pre>
     *
     * @author Marianne Mueller
     * @author Roland Schemers
     * @see java.security.CodeSource
     * @see java.security.Policy
     * @see java.security.Permissions
     * @see java.security.ProtectionDomain
     */
    private static class PolicyEntry {

        private final CodeSource codesource;
        final List<Permission> permissions;

        /**
         * Given a Permission and a CodeSource, create a policy entry.
         *
         * XXX Decide if/how to add validity fields and "purpose" fields to
         * XXX policy entries
         *
         * @param cs the CodeSource, which encapsulates the URL and the
         *           public key
         *           attributes from the policy config file. Validity checks
         *           are performed on the public key before PolicyEntry is
         *           called.
         *
         */
        PolicyEntry(CodeSource cs) {
            this.codesource = cs;
            this.permissions = new ArrayList<Permission>();
        }

        /**
         * add a Permission object to this entry.
         * No need to sync add op because perms are added to entry only
         * while entry is being initialized
         */
        void add(Permission p) {
            permissions.add(p);
        }

        /**
         * Return the CodeSource for this policy entry
         */
        CodeSource getCodeSource() {
            return codesource;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append(getCodeSource());
            sb.append("\n");
            for (int j = 0; j < permissions.size(); j++) {
                Permission p = permissions.get(j);
                sb.append(" ");
                sb.append(" ");
                sb.append(p);
                sb.append("\n");
            }
            sb.append("}");
            sb.append("\n");
            return sb.toString();
        }
    }

    /**
     * holds policy information that we need to synch on
     */
    private static class PolicyInfo {
        // Stores grant entries in the policy
        final List<PolicyEntry> policyEntries;

        PolicyInfo(int numCaches) {
            policyEntries = new ArrayList<>();
        }
    }

    @SuppressWarnings("deprecation")
    private static URL newURL(String spec) throws MalformedURLException {
        return new URL(spec);
    }
}
