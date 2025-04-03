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
import java.net.MalformedURLException;
import java.net.NetPermission;
import java.net.SocketPermission;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.SecurityPermission;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.PropertyPermission;
import java.util.Set;

@SuppressWarnings("removal")
public class PolicyFile extends java.security.Policy {
    public static final Set<String> PERM_CLASSES_TO_SKIP = Set.of(
        "org.opensearch.secure_sm.ThreadContextPermission",
        "org.opensearch.secure_sm.ThreadPermission",
        "org.opensearch.SpecialPermission",
        "org.bouncycastle.crypto.CryptoServicesPermission",
        "org.opensearch.script.ClassPermission",
        "javax.security.auth.AuthPermission"
    );

    private static final int DEFAULT_CACHE_SIZE = 1;
    private volatile PolicyInfo policyInfo;
    private URL url;

    public PolicyFile(URL url) {
        this.url = url;
        try {
            init(url);
        } catch (PolicyInitializationException e) {
            throw new RuntimeException("Failed to initialize policy file", e);
        }
    }

    private void init(URL url) throws PolicyInitializationException {
        int numCaches = DEFAULT_CACHE_SIZE;
        PolicyInfo newInfo = new PolicyInfo(numCaches);
        initPolicyFile(newInfo, url);
        policyInfo = newInfo;
    }

    private void initPolicyFile(final PolicyInfo newInfo, final URL url) throws PolicyInitializationException {
        init(url, newInfo);
    }

    private void init(URL policy, PolicyInfo newInfo) throws PolicyInitializationException {
        try (InputStreamReader reader = new InputStreamReader(getInputStream(policy), StandardCharsets.UTF_8)) {
            PolicyParser policyParser = new PolicyParser();
            policyParser.read(reader);

            for (GrantNode grantNode : Collections.list(policyParser.grantElements())) {
                addGrantNode(grantNode, newInfo);
            }

        } catch (Exception e) {
            throw new PolicyInitializationException("Failed to load policy from: " + policy, e);
        }
    }

    public static InputStream getInputStream(URL url) throws IOException {
        if ("file".equals(url.getProtocol())) {
            String path = url.getFile().replace('/', File.separatorChar);
            path = URLDecoder.decode(path, StandardCharsets.UTF_8);
            return new FileInputStream(path);
        } else {
            return url.openStream();
        }
    }

    private CodeSource getCodeSource(GrantNode grantEntry, PolicyInfo newInfo) throws PolicyInitializationException {
        try {
            Certificate[] certs = null;
            URL location = (grantEntry.codeBase != null) ? newURL(grantEntry.codeBase) : null;
            return canonicalizeCodebase(new CodeSource(location, certs));
        } catch (Exception e) {
            throw new PolicyInitializationException("Failed to get CodeSource", e);
        }
    }

    private void addGrantNode(GrantNode grantEntry, PolicyInfo newInfo) throws PolicyInitializationException {
        CodeSource codesource = getCodeSource(grantEntry, newInfo);
        if (codesource == null) {
            throw new PolicyInitializationException("Null CodeSource for: " + grantEntry.codeBase);
        }

        PolicyEntry entry = new PolicyEntry(codesource);
        Enumeration<PermissionNode> enum_ = grantEntry.permissionElements();
        while (enum_.hasMoreElements()) {
            PermissionNode pe = enum_.nextElement();
            expandPermissionName(pe);
            try {
                Optional<Permission> perm = getInstance(pe.permission, pe.name, pe.action);
                if (perm.isPresent()) {
                    entry.add(perm.get());
                }
            } catch (ClassNotFoundException e) {

                // these were mostly custom permission classes added for security
                // manager. Since security manager is deprecated, we can skip these
                // permissions classes.
                if (PERM_CLASSES_TO_SKIP.contains(pe.permission)) {
                    continue; // skip this permission
                }

                throw new PolicyInitializationException("Permission class not found: " + pe.permission, e);
            }
        }
        newInfo.policyEntries.add(entry);
    }

    private void expandPermissionName(PermissionNode pe) {
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

        return Optional.ofNullable(answer);
    }

    private static Permission getKnownPermission(Class<?> claz, String name, String actions) {
        if (claz.equals(FilePermission.class)) {
            return new FilePermission(name, actions);
        } else if (claz.equals(SocketPermission.class)) {
            return new SocketPermission(name, actions);
        } else if (claz.equals(RuntimePermission.class)) {
            return new RuntimePermission(name, actions);
        } else if (claz.equals(PropertyPermission.class)) {
            return new PropertyPermission(name, actions);
        } else if (claz.equals(NetPermission.class)) {
            return new NetPermission(name, actions);
        } else if (claz.equals(AllPermission.class)) {
            return new AllPermission();
        } else if (claz.equals(SecurityPermission.class)) {
            return new SecurityPermission(name, actions);
        } else {
            return null;
        }
    }

    @Override
    public void refresh() {
        try {
            init(url);
        } catch (PolicyInitializationException e) {
            throw new RuntimeException("Failed to refresh policy", e);
        }
    }

    @Override
    public boolean implies(ProtectionDomain pd, Permission p) {
        PermissionCollection pc = getPermissions(pd);
        return pc != null && pc.implies(p);
    }

    @Override
    public PermissionCollection getPermissions(ProtectionDomain domain) {
        Permissions perms = new Permissions();
        if (domain == null) return perms;

        try {
            getPermissionsForProtectionDomain(perms, domain);
        } catch (PolicyInitializationException e) {
            throw new RuntimeException("Failed to get permissions for domain", e);
        }

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
        if (codesource == null) return new Permissions();

        Permissions perms = new Permissions();
        CodeSource canonicalCodeSource;

        try {
            canonicalCodeSource = canonicalizeCodebase(codesource);
        } catch (PolicyInitializationException e) {
            throw new RuntimeException("Failed to canonicalize CodeSource", e);
        }

        for (PolicyEntry entry : policyInfo.policyEntries) {
            if (entry.getCodeSource().implies(canonicalCodeSource)) {
                for (Permission permission : entry.permissions) {
                    perms.add(permission);
                }
            }
        }

        return perms;
    }

    private void getPermissionsForProtectionDomain(Permissions perms, ProtectionDomain pd) throws PolicyInitializationException {
        final CodeSource cs = pd.getCodeSource();
        if (cs == null) return;

        CodeSource canonicalCodeSource = canonicalizeCodebase(cs);

        for (PolicyEntry entry : policyInfo.policyEntries) {
            if (entry.getCodeSource().implies(canonicalCodeSource)) {
                for (Permission permission : entry.permissions) {
                    perms.add(permission);
                }
            }
        }
    }

    private CodeSource canonicalizeCodebase(CodeSource cs) throws PolicyInitializationException {
        URL location = cs.getLocation();
        if (location == null) return cs;

        try {
            URL canonicalUrl = canonicalizeUrl(location);
            return new CodeSource(canonicalUrl, cs.getCertificates());
        } catch (IOException e) {
            throw new PolicyInitializationException("Failed to canonicalize CodeSource", e);
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
                    throw new IOException("Malformed nested jar URL", e);
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
            return new File(path).getCanonicalPath() + "*";
        } else {
            return new File(path).getCanonicalPath();
        }
    }

    private static class PolicyEntry {
        private final CodeSource codesource;
        final List<Permission> permissions;

        PolicyEntry(CodeSource cs) {
            this.codesource = cs;
            this.permissions = new ArrayList<>();
        }

        void add(Permission p) {
            permissions.add(p);
        }

        CodeSource getCodeSource() {
            return codesource;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").append(getCodeSource()).append("\n");
            for (Permission p : permissions) {
                sb.append("  ").append(p).append("\n");
            }
            sb.append("}\n");
            return sb.toString();
        }
    }

    private static class PolicyInfo {
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
