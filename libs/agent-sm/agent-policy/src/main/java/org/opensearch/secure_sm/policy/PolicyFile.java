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
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PropertyPermission;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@SuppressWarnings("removal")
public class PolicyFile extends java.security.Policy {
    public static final Set<String> PERM_CLASSES_TO_SKIP = Set.of(
        "org.opensearch.secure_sm.ThreadContextPermission",
        "org.opensearch.secure_sm.ThreadPermission",
        "org.opensearch.SpecialPermission",
        "org.bouncycastle.crypto.CryptoServicesPermission",
        "org.opensearch.script.ClassPermission",
        "javax.security.auth.AuthPermission",
        "javax.security.auth.kerberos.ServicePermission",
        "com.sun.tools.attach.AttachPermission"
    );

    private final PolicyInfo policyInfo;
    private final URL url;

    public PolicyFile(URL url) {
        this.url = url;
        try {
            policyInfo = init(url);
        } catch (PolicyInitializationException e) {
            throw new RuntimeException("Failed to initialize policy file", e);
        }
    }

    private PolicyInfo init(URL policy) throws PolicyInitializationException {
        List<PolicyEntry> entries = new ArrayList<>();
        try (InputStreamReader reader = new InputStreamReader(getInputStream(policy), StandardCharsets.UTF_8)) {
            List<GrantEntry> grantEntries = PolicyParser.read(reader);
            for (GrantEntry grantEntry : grantEntries) {
                addGrantEntry(grantEntry, entries);
            }
        } catch (Exception e) {
            throw new PolicyInitializationException("Failed to load policy from: " + policy, e);
        }

        return new PolicyInfo(entries);
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

    private CodeSource getCodeSource(GrantEntry grantEntry) throws PolicyInitializationException {
        try {
            Certificate[] certs = null;
            URL location = (grantEntry.codeBase() != null) ? newURL(grantEntry.codeBase()) : null;
            return canonicalizeCodebase(new CodeSource(location, certs));
        } catch (Exception e) {
            throw new PolicyInitializationException("Failed to get CodeSource", e);
        }
    }

    private void addGrantEntry(GrantEntry grantEntry, List<PolicyEntry> entries) throws PolicyInitializationException {
        CodeSource codesource = getCodeSource(grantEntry);
        if (codesource == null) {
            throw new PolicyInitializationException("Null CodeSource for: " + grantEntry.codeBase());
        }

        List<Permission> permissions = new ArrayList<>();
        for (PermissionEntry pe : grantEntry.permissionEntries()) {
            final PermissionEntry expandedEntry = expandPermissionName(pe);
            try {
                Optional<Permission> perm = getInstance(expandedEntry.permission(), expandedEntry.name(), expandedEntry.action());
                perm.ifPresent(permissions::add);
            } catch (ClassNotFoundException e) {
                // these were mostly custom permission classes added for security
                // manager. Since security manager is deprecated, we can skip these
                // permissions classes.
                if (PERM_CLASSES_TO_SKIP.contains(pe.permission())) {
                    continue;
                }
                throw new PolicyInitializationException("Permission class not found: " + pe.permission(), e);
            }
        }

        entries.add(new PolicyEntry(codesource, permissions));
    }

    /**
     * Expands known system properties like ${java.home} and ${user.home} to their absolute
     * path equivalents.
     */
    private static String expandKnownSystemProperty(final String property, final String value) {
        final int index = value.indexOf("${" + property + "}/");
        final String path = System.getProperty(property);
        if (path.endsWith(File.pathSeparator)) {
            return path + value.substring(index + property.length() + 4 /* replace the path separator */);
        } else {
            return path + value.substring(index + property.length() + 3 /* keep the path separator */);
        }
    }

    private static PermissionEntry expandPermissionName(PermissionEntry pe) {
        if (pe.name() == null) {
            return pe;
        }

        if (pe.name().startsWith("${java.home}")) {
            return new PermissionEntry(pe.permission(), expandKnownSystemProperty("java.home", pe.name()), pe.action());
        } else if (pe.name().startsWith("${user.home}")) {
            return new PermissionEntry(pe.permission(), expandKnownSystemProperty("user.home", pe.name()), pe.action());
        }

        if (!pe.name().contains("${{")) {
            return pe;
        }

        int startIndex = 0;
        int b, e;
        StringBuilder sb = new StringBuilder();

        while ((b = pe.name().indexOf("${{", startIndex)) != -1 && (e = pe.name().indexOf("}}", b)) != -1) {
            sb.append(pe.name(), startIndex, b);
            String value = pe.name().substring(b + 3, e);
            String propertyValue = System.getProperty(value);
            if (propertyValue != null) {
                sb.append(propertyValue);
            } else {
                // replacement not found
                sb.append("${{").append(value).append("}}");
            }
            startIndex = e + 2;
        }

        sb.append(pe.name().substring(startIndex));
        return new PermissionEntry(pe.permission(), sb.toString(), pe.action());
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
        if (pd == null || p == null) {
            return false;
        }

        PermissionCollection pc = policyInfo.getOrCompute(pd, this::getPermissions);
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
            if (entry.codeSource().implies(canonicalCodeSource)) {
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
            if (entry.codeSource().implies(canonicalCodeSource)) {
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

    private record PolicyEntry(CodeSource codeSource, List<Permission> permissions) {
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{").append(codeSource).append("\n");
            for (Permission p : permissions) {
                sb.append("  ").append(p).append("\n");
            }
            sb.append("}\n");
            return sb.toString();
        }
    }

    private static class PolicyInfo {
        private final List<PolicyEntry> policyEntries;
        private final Map<ProtectionDomain, PermissionCollection> pdMapping;

        PolicyInfo(List<PolicyEntry> entries) {
            this.policyEntries = List.copyOf(entries);  // an immutable copy for thread safety.
            this.pdMapping = new ConcurrentHashMap<>();
        }

        public PermissionCollection getOrCompute(ProtectionDomain pd, Function<ProtectionDomain, PermissionCollection> computeFn) {
            return pdMapping.computeIfAbsent(pd, k -> computeFn.apply(k));
        }
    }

    private static URL newURL(String spec) throws MalformedURLException, URISyntaxException {
        return new URI(spec).toURL();
    }
}
