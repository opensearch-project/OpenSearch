package org.opensearch.secure_sm.policy;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
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
import java.security.Principal;
import java.security.ProtectionDomain;
import java.security.Security;
import java.security.UnresolvedPermission;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
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
    private static final String SELF = "${{self}}";
    private static final String X500PRINCIPAL = "javax.security.auth.x500.X500Principal";
    private static final String POLICY = "java.security.policy";
    private static final String POLICY_URL = "policy.url.";

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
        pe.add(SecurityConstants.LOCAL_LISTEN_PERMISSION);

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

            PolicyEntry entry = new PolicyEntry(codesource, ge.principals);
            Enumeration<PolicyParser.PermissionEntry> enum_ = ge.permissionElements();
            while (enum_.hasMoreElements()) {
                PolicyParser.PermissionEntry pe = enum_.nextElement();

                try {
                    // perform ${{ ... }} expansions within permission name
                    expandPermissionName(pe);

                    // XXX special case PrivateCredentialPermission-SELF
                    Permission perm;
                    if (pe.permission.equals("javax.security.auth.PrivateCredentialPermission") && pe.name.endsWith(" self")) {
                        pe.name = pe.name.substring(0, pe.name.indexOf("self")) + SELF;
                    }
                    // check for self
                    if (pe.name != null && pe.name.contains(SELF)) {
                        // Create a "SelfPermission" , it could be an
                        // an unresolved permission which will be resolved
                        // when implies is called
                        // Add it to entry
                        perm = new SelfPermission(pe.permission, pe.name, pe.action);
                    } else {
                        perm = getInstance(pe.permission, pe.name, pe.action);
                    }
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
        } catch (Exception e) {
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
     * Creates one of the well-known principals in the java.base module
     * directly instead of via reflection. Keep list short to not penalize
     * principals from other modules.
     */
    private static Principal getKnownPrincipal(Class<?> claz, String name) {
        if (claz.equals(X500Principal.class)) {
            return new X500Principal(name);
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
        return getPermissions(perms, canonCodeSource, pd.getPrincipals());
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

        if (cs == null) return perms;

        CodeSource canonCodeSource = canonicalizeCodebase(cs);
        return getPermissions(perms, canonCodeSource, null);
    }

    private Permissions getPermissions(Permissions perms, final CodeSource cs, Principal[] principals) {
        for (PolicyEntry entry : policyInfo.policyEntries) {
            addPermissions(perms, cs, principals, entry);
        }

        return perms;
    }

    private void addPermissions(Permissions perms, final CodeSource cs, Principal[] principals, final PolicyEntry entry) {

        // check to see if the CodeSource implies
        Boolean imp = entry.getCodeSource().implies(cs);
        if (!imp.booleanValue()) {
            // CodeSource does not imply - return and try next policy entry
            return;
        }

        // check to see if the Principals imply

        List<PolicyParser.PrincipalEntry> entryPs = entry.getPrincipals();

        if (entryPs == null || entryPs.isEmpty()) {

            // policy entry has no principals -
            // add perms regardless of principals in current ACC

            addPerms(perms, principals, entry);
            return;

        } else if (principals == null || principals.length == 0) {

            // current thread has no principals but this policy entry
            // has principals - perms are not added

            return;
        }

        // current thread has principals and this policy entry
        // has principals. see if policy entry principals match
        // principals in current ACC

        for (PolicyParser.PrincipalEntry pppe : entryPs) {

            // Check for wildcards
            if (pppe.isWildcardClass()) {
                // a wildcard class matches all principals in current ACC
                continue;
            }

            if (pppe.isWildcardName()) {
                // a wildcard name matches any principal with the same class
                if (wildcardPrincipalNameImplies(pppe.principalClass, principals)) {
                    continue;
                }
                // policy entry principal not in current ACC -
                // immediately return and go to next policy entry
                return;
            }

            Set<Principal> pSet = new HashSet<>(Arrays.asList(principals));
            Subject subject = new Subject(true, pSet, Collections.EMPTY_SET, Collections.EMPTY_SET);
            try {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Class<?> pClass = Class.forName(pppe.principalClass, false, cl);
                Principal p = getKnownPrincipal(pClass, pppe.principalName);
                if (p == null) {
                    if (!Principal.class.isAssignableFrom(pClass)) {
                        // not the right subtype
                        throw new ClassCastException(pppe.principalClass + " is not a Principal");
                    }

                    Constructor<?> c = pClass.getConstructor(PARAMS1);
                    p = (Principal) c.newInstance(new Object[] { pppe.principalName });

                }

                // check if the Principal implies the current
                // thread's principals
                if (!p.implies(subject)) {
                    // policy principal does not imply the current Subject -
                    // immediately return and go to next policy entry
                    return;
                }
            } catch (Exception e) {
                // fall back to default principal comparison.
                // see if policy entry principal is in current ACC

                if (!pppe.implies(subject)) {
                    // policy entry principal not in current ACC -
                    // immediately return and go to next policy entry
                    return;
                }
            }

            // either the principal information matched,
            // or the Principal.implies succeeded.
            // continue loop and test the next policy principal
        }

        // all policy entry principals were found in the current ACC -
        // grant the policy permissions

        addPerms(perms, principals, entry);
    }

    /**
     * Returns true if the array of principals contains at least one
     * principal of the specified class.
     */
    private static boolean wildcardPrincipalNameImplies(String principalClass, Principal[] principals) {
        for (Principal p : principals) {
            if (principalClass.equals(p.getClass().getName())) {
                return true;
            }
        }
        return false;
    }

    private void addPerms(Permissions perms, Principal[] accPs, PolicyEntry entry) {
        for (int i = 0; i < entry.permissions.size(); i++) {
            Permission p = entry.permissions.get(i);

            if (p instanceof SelfPermission) {
                // handle "SELF" permissions
                expandSelf((SelfPermission) p, entry.getPrincipals(), accPs, perms);
            } else {
                perms.add(p);
            }
        }
    }

    /**
     * @param sp      the SelfPermission that needs to be expanded.
     *
     * @param entryPs list of principals for the Policy entry.
     *
     * @param pdp     Principal array from the current ProtectionDomain.
     *
     * @param perms   the PermissionCollection where the individual
     *                Permissions will be added after expansion.
     */

    private void expandSelf(SelfPermission sp, List<PolicyParser.PrincipalEntry> entryPs, Principal[] pdp, Permissions perms) {

        if (entryPs == null || entryPs.isEmpty()) {
            return;
        }
        int startIndex = 0;
        int v;
        StringBuilder sb = new StringBuilder();
        while ((v = sp.getSelfName().indexOf(SELF, startIndex)) != -1) {

            // add non-SELF string
            sb.append(sp.getSelfName().substring(startIndex, v));

            // expand SELF
            Iterator<PolicyParser.PrincipalEntry> pli = entryPs.iterator();
            while (pli.hasNext()) {
                PolicyParser.PrincipalEntry pppe = pli.next();
                String[][] principalInfo = getPrincipalInfo(pppe, pdp);
                for (int i = 0; i < principalInfo.length; i++) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(principalInfo[i][0] + " " + "\"" + principalInfo[i][1] + "\"");
                }
                if (pli.hasNext()) {
                    sb.append(", ");
                }
            }
            startIndex = v + SELF.length();
        }
        // add remaining string (might be the entire string)
        sb.append(sp.getSelfName().substring(startIndex));

        try {
            // first try to instantiate the permission
            perms.add(getInstance(sp.getSelfType(), sb.toString(), sp.getSelfActions()));
        } catch (ClassNotFoundException cnfe) {
            // ok, the permission is not in the bootclasspath.
            // before we add an UnresolvedPermission, check to see
            // whether this perm already belongs to the collection.
            // if so, use that perm's ClassLoader to create a new
            // one.
            Class<?> pc = null;
            synchronized (perms) {
                Enumeration<Permission> e = perms.elements();
                while (e.hasMoreElements()) {
                    Permission pElement = e.nextElement();
                    if (pElement.getClass().getName().equals(sp.getSelfType())) {
                        pc = pElement.getClass();
                        break;
                    }
                }
            }
            if (pc == null) {
                // create an UnresolvedPermission
                // FIX-ME
                Certificate certs[] = null;
                perms.add(new UnresolvedPermission(sp.getSelfType(), sb.toString(), sp.getSelfActions(), certs));
            } else {
                try {
                    // we found an instantiated permission.
                    // use its class loader to instantiate a new permission.
                    Constructor<?> c;
                    // name parameter can not be null
                    if (sp.getSelfActions() == null) {
                        try {
                            c = pc.getConstructor(PARAMS1);
                            perms.add((Permission) c.newInstance(new Object[] { sb.toString() }));
                        } catch (NoSuchMethodException ne) {
                            c = pc.getConstructor(PARAMS2);
                            perms.add((Permission) c.newInstance(new Object[] { sb.toString(), sp.getSelfActions() }));
                        }
                    } else {
                        c = pc.getConstructor(PARAMS2);
                        perms.add((Permission) c.newInstance(new Object[] { sb.toString(), sp.getSelfActions() }));
                    }
                } catch (Exception nme) {}
            }
        } catch (Exception e) {}
    }

    /**
     * return the principal class/name pair in the 2D array.
     * array[x][y]: x corresponds to the array length.
     * if (y == 0), it's the principal class.
     * if (y == 1), it's the principal name.
     */
    private String[][] getPrincipalInfo(PolicyParser.PrincipalEntry pe, Principal[] pdp) {

        // there are 3 possibilities:
        // 1) the entry's Principal class and name are not wildcarded
        // 2) the entry's Principal name is wildcarded only
        // 3) the entry's Principal class and name are wildcarded

        if (!pe.isWildcardClass() && !pe.isWildcardName()) {

            // build an info array for the principal
            // from the Policy entry
            String[][] info = new String[1][2];
            info[0][0] = pe.principalClass;
            info[0][1] = pe.principalName;
            return info;

        } else if (!pe.isWildcardClass() && pe.isWildcardName()) {

            // build an info array for every principal
            // in the current domain which has a principal class
            // that is equal to policy entry principal class name
            List<Principal> plist = new ArrayList<>();
            for (int i = 0; i < pdp.length; i++) {
                if (pe.principalClass.equals(pdp[i].getClass().getName())) plist.add(pdp[i]);
            }
            String[][] info = new String[plist.size()][2];
            int i = 0;
            for (Principal p : plist) {
                info[i][0] = p.getClass().getName();
                info[i][1] = p.getName();
                i++;
            }
            return info;

        } else {

            // build an info array for every
            // one of the current Domain's principals

            String[][] info = new String[pdp.length][2];

            for (int i = 0; i < pdp.length; i++) {
                info[i][0] = pdp[i].getClass().getName();
                info[i][1] = pdp[i].getName();
            }
            return info;
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

            // handle different prefix possibilities
            if (prefix.equalsIgnoreCase("self")) {
                // do nothing - handled later
                sb.append(pe.name.substring(b, e + 2));
                startIndex = e + 2;
                continue;
            } else if (prefix.equalsIgnoreCase("alias")) {
                // get the suffix and perform keystore alias replacement
                if (colonIndex == -1) {
                    throw new Exception("Alias name not provided pe.name: " + pe.name);
                }
                suffix = value.substring(colonIndex + 1);

                sb.append(X500PRINCIPAL + " \"" + suffix + "\"");
                startIndex = e + 2;
            } else {
                throw new Exception("Substitution value prefix unsupported: " + prefix);
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
        private final List<PolicyParser.PrincipalEntry> principals;

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
        PolicyEntry(CodeSource cs, List<PolicyParser.PrincipalEntry> principals) {
            this.codesource = cs;
            this.permissions = new ArrayList<Permission>();
            this.principals = principals; // can be null
        }

        PolicyEntry(CodeSource cs) {
            this(cs, null);
        }

        List<PolicyParser.PrincipalEntry> getPrincipals() {
            return principals; // can be null
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

    private static class SelfPermission extends Permission {

        @java.io.Serial
        private static final long serialVersionUID = -8315562579967246806L;

        /**
         * The class name of the Permission class that will be
         * created when this self permission is expanded .
         *
         * @serial
         */
        private String type;

        /**
         * The permission name.
         *
         * @serial
         */
        private String name;

        /**
         * The actions of the permission.
         *
         * @serial
         */
        private String actions;

        /**
         * Creates a new SelfPermission containing the permission
         * information needed later to expand the self
         *
         * @param type    the class name of the Permission class that will be
         *                created when this permission is expanded and if necessary
         *                resolved.
         * @param name    the name of the permission.
         * @param actions the actions of the permission.
         *                This is a list of certificate chains, where each chain is
         *                composed of
         *                a signer certificate and optionally its supporting certificate
         *                chain.
         *                Each chain is ordered bottom-to-top (i.e., with the signer
         *                certificate first and the (root) certificate authority last).
         */
        public SelfPermission(String type, String name, String actions) {
            super(type);
            if (type == null) {
                throw new NullPointerException("Ttype cannot be null");
            }
            this.type = type;
            this.name = name;
            this.actions = actions;
        }

        /**
         * This method always returns false for SelfPermission permissions.
         * That is, an SelfPermission never considered to
         * imply another permission.
         *
         * @param p the permission to check against.
         *
         * @return false.
         */
        @Override
        public boolean implies(Permission p) {
            return false;
        }

        /**
         * Checks two SelfPermission objects for equality.
         *
         * Checks that <i>obj</i> is an SelfPermission, and has
         * the same type (class) name, permission name, actions, and
         * certificates as this object.
         *
         * @param obj the object we are testing for equality with this object.
         *
         * @return true if obj is an SelfPermission, and has the same
         *         type (class) name, permission name, actions, and
         *         certificates as this object.
         */
        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;

            if (!(obj instanceof SelfPermission)) return false;
            SelfPermission that = (SelfPermission) obj;

            if (!(this.type.equals(that.type) && this.name.equals(that.name) && this.actions.equals(that.actions))) return false;

            return true;
        }

        /**
         * Returns the hash code value for this object.
         *
         * @return a hash code value for this object.
         */
        @Override
        public int hashCode() {
            int hash = type.hashCode();
            if (name != null) hash ^= name.hashCode();
            if (actions != null) hash ^= actions.hashCode();
            return hash;
        }

        /**
         * Returns the canonical string representation of the actions,
         * which currently is the empty string "", since there are no actions
         * for an SelfPermission. That is, the actions for the
         * permission that will be created when this SelfPermission
         * is resolved may be non-null, but an SelfPermission
         * itself is never considered to have any actions.
         *
         * @return the empty string "".
         */
        @Override
        public String getActions() {
            return "";
        }

        public String getSelfType() {
            return type;
        }

        public String getSelfName() {
            return name;
        }

        public String getSelfActions() {
            return actions;
        }

        /**
         * Returns a string describing this SelfPermission. The convention
         * is to specify the class name, the permission name, and the actions,
         * in the following format: '(unresolved "ClassName" "name" "actions")'.
         *
         * @return information about this SelfPermission.
         */
        @Override
        public String toString() {
            return "(SelfPermission " + type + " " + name + " " + actions + ")";
        }

        /**
         * Restores the state of this object from the stream.
         *
         * @param stream the {@code ObjectInputStream} from which data is read
         * @throws IOException            if an I/O error occurs
         * @throws ClassNotFoundException if a serialized class cannot be loaded
         */
        @java.io.Serial
        private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
            stream.defaultReadObject();
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
