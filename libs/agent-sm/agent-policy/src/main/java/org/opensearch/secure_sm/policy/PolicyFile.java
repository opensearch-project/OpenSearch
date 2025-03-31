/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.secure_sm.policy;

import org.opensearch.secure_sm.policy.PolicyParser.PermissionEntry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("removal")
public class PolicyFile extends java.security.Policy {
    public static final SocketPermission LOCAL_LISTEN_PERMISSION = new SocketPermission("localhost:0", "listen");

    private static final int DEFAULT_CACHE_SIZE = 1;
    private volatile PolicyInfo policyInfo;
    private URL url;

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
        init(url, newInfo);
    }

    /**
     * Reads a policy configuration into the Policy object using a
     * Reader object.
     */
    private void init(URL policy, PolicyInfo newInfo) {
        if (badPolicyURLs.contains(policy)) {
            return;
        }

        try (InputStreamReader reader = new InputStreamReader(getInputStream(policy), StandardCharsets.UTF_8)) {
            PolicyParser policyParser = new PolicyParser();
            policyParser.read(reader);

            Collections.list(policyParser.grantElements()).forEach(grantNode -> {
                try {
                    addGrantEntry(grantNode, newInfo);
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                }
            });

            return;
        } catch (PolicyParser.ParsingException pe) {
            badPolicyURLs.add(policy);
            pe.printStackTrace(System.err);
        } catch (IOException ioe) {
            ioe.printStackTrace(System.err);
        }

        return;
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

    private void addGrantEntry(PolicyParser.GrantEntry ge, PolicyInfo newInfo) throws Exception {

        CodeSource codesource = getCodeSource(ge, newInfo);
        if (codesource == null) return;

        PolicyEntry entry = new PolicyEntry(codesource);
        Enumeration<PolicyParser.PermissionEntry> enum_ = ge.permissionElements();
        while (enum_.hasMoreElements()) {
            PolicyParser.PermissionEntry pe = enum_.nextElement();
            try {
                // Store the original name before expansion
                expandPermissionName(pe);

                Optional<Permission> perm = getInstance(pe.permission, pe.name, pe.action);
                perm.ifPresent(entry::add);
            } catch (ClassNotFoundException cfne) {
                cfne.printStackTrace(System.err);
            }
        }
        newInfo.policyEntries.add(entry);
    }

    private void expandPermissionName(PermissionEntry pe) {
        if (pe.name == null || !pe.name.contains("${{")) {
            return;
        }

        int startIndex = 0;
        int b, e;
        StringBuilder sb = new StringBuilder();

        while ((b = pe.name.indexOf("${{", startIndex)) != -1 && (e = pe.name.indexOf("}}", b)) != -1) {

            sb.append(pe.name, startIndex, b);
            String value = pe.name.substring(b + 3, e);

            sb.append("${{").append(value).append("}}");

            startIndex = e + 2;
        }

        sb.append(pe.name.substring(startIndex));
        pe.name = sb.toString();
    }

    private static final Optional<Permission> getInstance(String type, String name, String actions) throws ClassNotFoundException {
        Class<?> pc = Class.forName(type, false, null);
        Permission answer = getKnownPermission(pc, name, actions);
        if (answer != null) {
            return Optional.of(answer);
        }

        return Optional.empty();
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

    @Override
    public boolean implies(ProtectionDomain pd, Permission p) {
        PermissionCollection pc = getPermissions(pd);
        if (pc == null) {
            return false;
        }

        // cache mapping of protection domain to its PermissionCollection
        return pc.implies(p);
    }

    @Override
    public PermissionCollection getPermissions(ProtectionDomain domain) {
        Permissions perms = new Permissions();

        if (domain == null) return perms;

        getPermissionsForProtectionDomain(perms, domain);

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

    @Override
    public PermissionCollection getPermissions(CodeSource codesource) {
        if (codesource == null) {
            return new Permissions();
        }

        Permissions perms = new Permissions();
        CodeSource canonicalCodeSource = canonicalizeCodebase(codesource);

        for (PolicyEntry entry : policyInfo.policyEntries) {
            if (entry.getCodeSource().implies(canonicalCodeSource)) {
                for (Permission permission : entry.permissions) {
                    perms.add(permission);
                }
            }
        }

        return perms;
    }

    private PermissionCollection getPermissionsForProtectionDomain(Permissions perms, ProtectionDomain pd) {
        final CodeSource cs = pd.getCodeSource();
        if (cs == null) return perms;

        for (PolicyEntry entry : policyInfo.policyEntries) {
            if (entry.getCodeSource().implies(cs)) {
                for (Permission permission : entry.permissions) {
                    perms.add(permission);
                }
            }
        }

        return perms;
    }

    private CodeSource canonicalizeCodebase(CodeSource cs) {
        URL location = cs.getLocation();
        if (location == null) {
            return cs;
        }

        try {
            URL canonicalUrl = canonicalizeUrl(location);
            return new CodeSource(canonicalUrl, cs.getCertificates());
        } catch (IOException e) {
            // Log the exception or handle it as appropriate
            return cs;
        }
    }

    @SuppressWarnings("deprecation")
    private URL canonicalizeUrl(URL url) throws IOException {
        String protocol = url.getProtocol();

        if ("jar".equals(protocol)) {
            String spec = url.getFile();
            int separator = spec.indexOf("!/");
            if (separator != -1) {
                try {
                    url = new URL(spec.substring(0, separator));
                } catch (MalformedURLException e) {
                    // If unwrapping fails, keep the original URL
                }
            }
        }

        if ("file".equals(url.getProtocol())) {
            String path = url.getPath();
            path = canonicalizePath(path);
            return new File(path).toURI().toURL();
        }

        return url;
    }

    private String canonicalizePath(String path) throws IOException {
        if (path.endsWith("*")) {
            path = path.substring(0, path.length() - 1);
            String canonicalPath = new File(path).getCanonicalPath();
            return canonicalPath + "*";
        } else {
            return new File(path).getCanonicalPath();
        }
    }

    private static class PolicyEntry {

        private final CodeSource codesource;
        final List<Permission> permissions;

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
