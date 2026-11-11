/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tools.launchers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Generates the {@code java.security} file that an OpenSearch JVM loads as a complete replacement of the JDK's own file
 * ({@code -Djava.security.properties==<file>}), and prints its path to standard output.
 * <p>
 * The file starts from the {@code java.security} of the JDK that runs this class, which is the JDK that will run OpenSearch,
 * so every JDK default except the provider list carries over unchanged:
 * <ul>
 *     <li>FIPS mode: the JDK's providers are dropped and the providers declared in {@code fips_java.security} in the config
 *     directory take their place. Any other key in that file overrides the JDK default.</li>
 *     <li>Default mode: the JDK's providers stay as they are, and the BouncyCastle FIPS provider is declared in the next free
 *     slot, so that BCFKS key stores work without FIPS mode.</li>
 * </ul>
 * Replacing the whole file, rather than appending to it, is what removes the JDK's providers in FIPS mode. JCA has no way to
 * unregister a provider declaratively, and an appended file can only add or overwrite entries.
 * <p>
 * The file is written to a directory that only the current user can access, and is named after a hash of its content. Each
 * combination of JDK, mode and config therefore produces one file, which later starts reuse. Nothing is generated when
 * {@code OPENSEARCH_JAVA_OPTS} already sets {@code java.security.properties}.
 */
final class JavaSecurityFile {

    static final String FIPS_MODE = "fips";
    static final String DEFAULT_MODE = "default";
    static final String FIPS_SECURITY_FILE_NAME = "fips_java.security";
    static final String PROVIDER_KEY_PREFIX = "security.provider.";
    static final String BC_FIPS_PROVIDER_NAME = "BCFIPS";
    static final String BC_FIPS_PROVIDER_CLASS = "org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider";
    static final String BC_FIPS_PROVIDER_ENTRY = BC_FIPS_PROVIDER_CLASS + " C:HYBRID;ENABLE{All};";

    private static final String JAVA_SECURITY_PROPERTIES_OPTION = "-Djava.security.properties=";
    private static final String DIRECTORY_PREFIX = "opensearch-";
    private static final String FILE_PREFIX = "java.security-";
    private static final Set<PosixFilePermission> OWNER_ONLY = EnumSet.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE
    );

    private JavaSecurityFile() {}

    /**
     * The main entry point. Prints the path of the generated file, or nothing when the user configures
     * {@code java.security.properties} through {@code OPENSEARCH_JAVA_OPTS}.
     *
     * @param args the config directory, and the mode ({@value #FIPS_MODE} or {@value #DEFAULT_MODE})
     * @throws IOException if the JDK's file or the FIPS file cannot be read, or the file cannot be written
     */
    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("expected two arguments [config directory, mode] but was " + Arrays.toString(args));
        }
        if (isConfiguredByUser(System.getenv("OPENSEARCH_JAVA_OPTS"))) {
            return;
        }
        final Path configDirectory = Paths.get(args[0]);
        final String mode = args[1];
        final Path jdkFile = Paths.get(System.getProperty("java.home"), "conf", "security", "java.security");
        final Properties jdkProperties = load(jdkFile);

        final Properties properties;
        final String sources;
        if (FIPS_MODE.equals(mode)) {
            final Path fipsFile = configDirectory.resolve(FIPS_SECURITY_FILE_NAME);
            if (Files.isRegularFile(fipsFile) == false) {
                throw new IllegalStateException("FIPS mode requires the provider list in [" + fipsFile + "], but the file does not exist");
            }
            properties = forFipsMode(jdkProperties, load(fipsFile));
            sources = jdkFile + " (without its providers) and " + fipsFile;
        } else if (DEFAULT_MODE.equals(mode)) {
            properties = forDefaultMode(jdkProperties, isClassAvailable(BC_FIPS_PROVIDER_CLASS));
            sources = jdkFile.toString();
        } else {
            throw new IllegalArgumentException("unknown mode [" + mode + "], expected [" + FIPS_MODE + "] or [" + DEFAULT_MODE + "]");
        }

        final byte[] content = render(properties, sources);
        final Path file = writeIfChanged(privateDirectory(temporaryDirectory()), content);
        Launchers.outPrintln(file.toString());
    }

    static boolean isConfiguredByUser(final String javaOpts) {
        if (javaOpts == null) {
            return false;
        }
        return Arrays.stream(javaOpts.split("\\s+")).anyMatch(option -> option.startsWith(JAVA_SECURITY_PROPERTIES_OPTION));
    }

    /**
     * The JDK's properties without its providers, overridden by every key of the FIPS file.
     */
    static Properties forFipsMode(final Properties jdkProperties, final Properties fipsProperties) {
        final Properties result = new Properties();
        for (final String key : jdkProperties.stringPropertyNames()) {
            final boolean isProvider = key.startsWith(PROVIDER_KEY_PREFIX);
            if (isProvider == false) {
                result.setProperty(key, jdkProperties.getProperty(key));
            }
        }
        for (final String key : fipsProperties.stringPropertyNames()) {
            result.setProperty(key, fipsProperties.getProperty(key));
        }
        final int providerCount = verifiedProviderCount(result);
        if (providerCount == 0) {
            throw new IllegalStateException("FIPS mode requires at least [" + PROVIDER_KEY_PREFIX + "1] in " + FIPS_SECURITY_FILE_NAME);
        }
        return result;
    }

    /**
     * The JDK's properties, with the BouncyCastle FIPS provider declared after the JDK's providers when it is on the classpath.
     */
    static Properties forDefaultMode(final Properties jdkProperties, final boolean bcFipsAvailable) {
        final Properties result = new Properties();
        for (final String key : jdkProperties.stringPropertyNames()) {
            result.setProperty(key, jdkProperties.getProperty(key));
        }
        final int providerCount = verifiedProviderCount(result);
        if (bcFipsAvailable && declaresBcFips(result, providerCount) == false) {
            final int nextFreeSlot = providerCount + 1;
            result.setProperty(PROVIDER_KEY_PREFIX + nextFreeSlot, BC_FIPS_PROVIDER_ENTRY);
        }
        return result;
    }

    /**
     * Counts the providers the JDK will load. JCA stops at the first slot that is missing or blank, so a provider declared after
     * such a gap would be ignored without notice. That is rejected here.
     */
    static int verifiedProviderCount(final Properties properties) {
        int count = 0;
        while (true) {
            final String nextProviderEntry = properties.getProperty(PROVIDER_KEY_PREFIX + (count + 1));
            if (isBlank(nextProviderEntry)) {
                break;
            }
            count++;
        }
        for (final String key : properties.stringPropertyNames()) {
            if (key.startsWith(PROVIDER_KEY_PREFIX) == false) {
                continue;
            }
            final String slot = key.substring(PROVIDER_KEY_PREFIX.length());
            final boolean isLoaded = isSlotNumber(slot) && Integer.parseInt(slot) <= count;
            if (isLoaded == false && isBlank(properties.getProperty(key)) == false) {
                throw new IllegalStateException(
                    "["
                        + key
                        + "] would be ignored, because providers must be numbered consecutively from 1, but only 1 to "
                        + count
                        + " are"
                );
            }
        }
        return count;
    }

    private static boolean declaresBcFips(final Properties properties, final int providerCount) {
        for (int slot = 1; slot <= providerCount; slot++) {
            final String entry = properties.getProperty(PROVIDER_KEY_PREFIX + slot).trim();
            final String providerNameOrClass = entry.split("\\s+")[0];
            if (providerNameOrClass.equals(BC_FIPS_PROVIDER_NAME) || providerNameOrClass.equals(BC_FIPS_PROVIDER_CLASS)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Renders the properties in the format the JDK reads, with the providers first in slot order and all other keys sorted, so the
     * same input always produces the same bytes.
     */
    static byte[] render(final Properties properties, final String sources) {
        final StringBuilder text = new StringBuilder();
        text.append("# Generated by OpenSearch from ").append(sources).append('\n');
        text.append("# Do not edit: OpenSearch generates this file again on every start.\n");
        final int providerCount = verifiedProviderCount(properties);
        for (int slot = 1; slot <= providerCount; slot++) {
            final String key = PROVIDER_KEY_PREFIX + slot;
            text.append(escapedEntry(key, properties.getProperty(key)));
        }
        final Set<String> otherKeys = new TreeSet<>(properties.stringPropertyNames());
        for (int slot = 1; slot <= providerCount; slot++) {
            otherKeys.remove(PROVIDER_KEY_PREFIX + slot);
        }
        for (final String key : otherKeys) {
            text.append(escapedEntry(key, properties.getProperty(key)));
        }
        return text.toString().getBytes(StandardCharsets.ISO_8859_1);
    }

    /**
     * Escapes one entry with {@link Properties#store}, the counterpart of the {@link Properties#load} the JDK reads the file with.
     */
    private static String escapedEntry(final String key, final String value) {
        final Properties single = new Properties();
        single.setProperty(key, value);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try {
            single.store(bytes, null);
        } catch (final IOException e) {
            throw new IllegalStateException("writing to memory failed", e);
        }
        final String stored = new String(bytes.toByteArray(), StandardCharsets.ISO_8859_1);
        // store() always starts with a comment line holding the current date
        final String withoutDateLine = stored.substring(stored.indexOf('\n') + 1);
        return withoutDateLine.replace("\r\n", "\n");
    }

    private static Path temporaryDirectory() {
        final String tmpdir = System.getenv("TMPDIR");
        if (isBlank(tmpdir) == false) {
            return Paths.get(tmpdir);
        }
        return Paths.get(System.getProperty("java.io.tmpdir"));
    }

    /**
     * Returns {@code <base>/opensearch-<uid>} (or {@code opensearch-<user name>} without POSIX user ids), creates it with owner-only
     * permissions if needed, and verifies that it belongs to the current user and that nobody else can access it. Otherwise another
     * local user could place a file there that the JVM would load as its security configuration.
     */
    static Path privateDirectory(final Path base) throws IOException {
        final boolean hasPosixPermissions = base.getFileSystem().supportedFileAttributeViews().contains("posix");
        final boolean hasUnixIds = base.getFileSystem().supportedFileAttributeViews().contains("unix");

        // the owner of a file we just created is the current user, which avoids looking up the user by name
        final UserPrincipal currentUser;
        final String userSuffix;
        final Path probe = Files.createTempFile(base, DIRECTORY_PREFIX, ".probe");
        try {
            currentUser = Files.getOwner(probe);
            if (hasUnixIds) {
                userSuffix = String.valueOf(Files.getAttribute(probe, "unix:uid"));
            } else {
                userSuffix = currentUser.getName().replaceAll("[^A-Za-z0-9._-]", "_");
            }
        } finally {
            Files.deleteIfExists(probe);
        }

        final Path directory = base.resolve(DIRECTORY_PREFIX + userSuffix);
        if (Files.notExists(directory, LinkOption.NOFOLLOW_LINKS)) {
            try {
                if (hasPosixPermissions) {
                    Files.createDirectory(directory, PosixFilePermissions.asFileAttribute(OWNER_ONLY));
                } else {
                    Files.createDirectory(directory);
                }
            } catch (final FileAlreadyExistsException e) {
                // another launcher created it concurrently; it is verified below like any existing directory
            }
        }
        verifyPrivate(directory, currentUser, hasPosixPermissions);
        return directory;
    }

    private static void verifyPrivate(final Path directory, final UserPrincipal currentUser, final boolean hasPosixPermissions)
        throws IOException {
        if (Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) == false) {
            throw new IOException("[" + directory + "] must be a directory, but is a file or a symbolic link; remove it");
        }
        final UserPrincipal owner = Files.getOwner(directory, LinkOption.NOFOLLOW_LINKS);
        if (owner.equals(currentUser) == false) {
            throw new IOException(
                "[" + directory + "] must belong to [" + currentUser.getName() + "], but belongs to [" + owner.getName() + "]"
            );
        }
        if (hasPosixPermissions) {
            final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(directory, LinkOption.NOFOLLOW_LINKS);
            if (OWNER_ONLY.containsAll(permissions) == false) {
                throw new IOException(
                    "["
                        + directory
                        + "] must only be accessible by its owner, but has permissions ["
                        + PosixFilePermissions.toString(permissions)
                        + "]; restrict it with chmod 700"
                );
            }
        }
    }

    /**
     * Writes the content to a file named after its hash, unless that file already holds exactly this content.
     */
    static Path writeIfChanged(final Path directory, final byte[] content) throws IOException {
        final String name = FILE_PREFIX + sha256Hex(content).substring(0, 32);
        final Path file = directory.resolve(name);
        final boolean isUpToDate = Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) && Arrays.equals(Files.readAllBytes(file), content);
        if (isUpToDate) {
            return file;
        }
        final Path temporaryFile = Files.createTempFile(directory, name, ".tmp");
        try {
            Files.write(temporaryFile, content);
            Files.move(temporaryFile, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally {
            Files.deleteIfExists(temporaryFile);
        }
        return file;
    }

    private static Properties load(final Path file) throws IOException {
        final Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(file)) {
            // ISO-8859-1, like the JDK reads java.security
            properties.load(in);
        }
        return properties;
    }

    private static boolean isClassAvailable(final String className) {
        try {
            Class.forName(className, false, JavaSecurityFile.class.getClassLoader());
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }

    private static String sha256Hex(final byte[] content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private static boolean isSlotNumber(final String slot) {
        return slot.isEmpty() == false && slot.chars().allMatch(Character::isDigit) && slot.length() < 10;
    }

    private static boolean isBlank(final String value) {
        return value == null || value.trim().isEmpty();
    }
}
