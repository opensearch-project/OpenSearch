/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugins;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.Constants;
import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.bootstrap.JarHell;
import org.opensearch.cli.EnvironmentAwareCommand;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.hash.MessageDigests;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.opensearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the plugin cli to install a plugin into opensearch.
 * <p>
 * The install command takes a plugin id, which may be any of the following:
 * <ul>
 * <li>An official opensearch plugin name</li>
 * <li>Maven coordinates to a plugin zip</li>
 * <li>A URL to a plugin zip</li>
 * </ul>
 *
 * Plugins are packaged as zip files. Each packaged plugin must contain a plugin properties file.
 * See {@link PluginInfo}.
 * <p>
 * The installation process first extracts the plugin files into a temporary
 * directory in order to verify the plugin satisfies the following requirements:
 * <ul>
 * <li>Jar hell does not exist, either between the plugin's own jars, or with opensearch</li>
 * <li>The plugin is not a module already provided with opensearch</li>
 * <li>If the plugin contains extra security permissions, the policy file is validated</li>
 * </ul>
 * <p>
 * A plugin may also contain an optional {@code bin} directory which contains scripts. The
 * scripts will be installed into a subdirectory of the opensearch bin directory, using
 * the name of the plugin, and the scripts will be marked executable.
 * <p>
 * A plugin may also contain an optional {@code config} directory which contains configuration
 * files specific to the plugin. The config files be installed into a subdirectory of the
 * opensearch config directory, using the name of the plugin. If any files to be installed
 * already exist, they will be skipped.
 */
class InstallPluginCommand extends EnvironmentAwareCommand {

    // exit codes for install
    /** A plugin with the same name is already installed. */
    static final int PLUGIN_EXISTS = 1;
    /** The plugin zip is not properly structured. */
    static final int PLUGIN_MALFORMED = 2;

    /** The builtin modules, which are plugins, but cannot be installed or removed. */
    static final Set<String> MODULES;
    static {
        try (
            InputStream stream = InstallPluginCommand.class.getResourceAsStream("/modules.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        ) {
            Set<String> modules = new HashSet<>();
            String line = reader.readLine();
            while (line != null) {
                modules.add(line.trim());
                line = reader.readLine();
            }
            MODULES = Collections.unmodifiableSet(modules);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** The official plugins that can be installed simply by name. */
    static final Set<String> OFFICIAL_PLUGINS;
    static {
        try (
            InputStream stream = InstallPluginCommand.class.getResourceAsStream("/plugins.txt");
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
        ) {
            Set<String> plugins = new TreeSet<>(); // use tree set to get sorting for help command
            String line = reader.readLine();
            while (line != null) {
                plugins.add(line.trim());
                line = reader.readLine();
            }
            OFFICIAL_PLUGINS = Collections.unmodifiableSet(plugins);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final OptionSpec<Void> batchOption;
    private final OptionSpec<String> arguments;

    static final Set<PosixFilePermission> BIN_DIR_PERMS;
    static final Set<PosixFilePermission> BIN_FILES_PERMS;
    static final Set<PosixFilePermission> CONFIG_DIR_PERMS;
    static final Set<PosixFilePermission> CONFIG_FILES_PERMS;
    static final Set<PosixFilePermission> PLUGIN_DIR_PERMS;
    static final Set<PosixFilePermission> PLUGIN_FILES_PERMS;

    static {
        // Bin directory get chmod 755
        BIN_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-xr-x"));

        // Bin files also get chmod 755
        BIN_FILES_PERMS = BIN_DIR_PERMS;

        // Config directory get chmod 750
        CONFIG_DIR_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rwxr-x---"));

        // Config files get chmod 660
        CONFIG_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-rw----"));

        // Plugin directory get chmod 755
        PLUGIN_DIR_PERMS = BIN_DIR_PERMS;

        // Plugins files get chmod 644
        PLUGIN_FILES_PERMS = Collections.unmodifiableSet(PosixFilePermissions.fromString("rw-r--r--"));
    }

    InstallPluginCommand() {
        super("Install a plugin");
        this.batchOption = parser.acceptsAll(
            Arrays.asList("b", "batch"),
            "Enable batch mode explicitly, automatic confirmation of security permission"
        );
        this.arguments = parser.nonOptions("plugin <name|Zip File|URL>");
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("Plugins are packaged as zip files. Each packaged plugin must contain a plugin properties file.");
        terminal.println("");

        // List possible plugin id inputs
        terminal.println("The install command takes a plugin id, which may be any of the following:");
        terminal.println("  An official opensearch plugin name");
        terminal.println("  Maven coordinates to a plugin zip");
        terminal.println("  A URL to a plugin zip");
        terminal.println("  A local zip file");
        terminal.println("");

        // List official opensearch plugin names
        terminal.println("The following official plugins may be installed by name:");
        for (String plugin : OFFICIAL_PLUGINS) {
            terminal.println("  " + plugin);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        List<String> pluginId = arguments.values(options);
        final boolean isBatch = options.has(batchOption);
        execute(terminal, pluginId, isBatch, env);
    }

    // pkg private for testing
    void execute(Terminal terminal, List<String> pluginIds, boolean isBatch, Environment env) throws Exception {
        if (pluginIds.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "at least one plugin id is required");
        }

        final Set<String> uniquePluginIds = new HashSet<>();
        for (final String pluginId : pluginIds) {
            if (uniquePluginIds.add(pluginId) == false) {
                throw new UserException(ExitCodes.USAGE, "duplicate plugin id [" + pluginId + "]");
            }
        }

        final Map<String, List<Path>> deleteOnFailures = new LinkedHashMap<>();
        for (final String pluginId : pluginIds) {
            terminal.println("-> Installing " + pluginId);
            try {
                final List<Path> deleteOnFailure = new ArrayList<>();
                deleteOnFailures.put(pluginId, deleteOnFailure);

                final Path pluginZip = download(terminal, pluginId, env.tmpDir(), isBatch);
                final Path extractedZip = unzip(pluginZip, env.pluginsDir());
                deleteOnFailure.add(extractedZip);
                final PluginInfo pluginInfo = installPlugin(terminal, isBatch, extractedZip, env, deleteOnFailure);
                terminal.println("-> Installed " + pluginInfo.getName() + " with folder name " + pluginInfo.getTargetFolderName());
                // swap the entry by plugin id for one with the installed plugin name, it gives a cleaner error message for URL installs
                deleteOnFailures.remove(pluginId);
                deleteOnFailures.put(pluginInfo.getName(), deleteOnFailure);
            } catch (final Exception installProblem) {
                terminal.println("-> Failed installing " + pluginId);
                for (final Map.Entry<String, List<Path>> deleteOnFailureEntry : deleteOnFailures.entrySet()) {
                    terminal.println("-> Rolling back " + deleteOnFailureEntry.getKey());
                    boolean success = false;
                    try {
                        IOUtils.rm(deleteOnFailureEntry.getValue().toArray(new Path[0]));
                        success = true;
                    } catch (final IOException exceptionWhileRemovingFiles) {
                        final Exception exception = new Exception(
                            "failed rolling back installation of [" + deleteOnFailureEntry.getKey() + "]",
                            exceptionWhileRemovingFiles
                        );
                        installProblem.addSuppressed(exception);
                        terminal.println("-> Failed rolling back " + deleteOnFailureEntry.getKey());
                    }
                    if (success) {
                        terminal.println("-> Rolled back " + deleteOnFailureEntry.getKey());
                    }
                }
                throw installProblem;
            }
        }
    }

    /** Downloads the plugin and returns the file it was downloaded to. */
    private Path download(Terminal terminal, String pluginId, Path tmpDir, boolean isBatch) throws Exception {

        if (OFFICIAL_PLUGINS.contains(pluginId)) {
            final String url = getOpenSearchUrl(terminal, Version.CURRENT, isSnapshot(), pluginId, Platforms.PLATFORM_NAME);
            terminal.println("-> Downloading " + pluginId + " from opensearch");
            return downloadAndValidate(terminal, url, tmpDir, true, isBatch);
        }

        // now try as maven coordinates, a valid URL would only have a colon and slash
        String[] coordinates = pluginId.split(":");
        if (coordinates.length == 3 && pluginId.contains("/") == false && pluginId.startsWith("file:") == false) {
            String mavenUrl = getMavenUrl(terminal, coordinates, Platforms.PLATFORM_NAME);
            terminal.println("-> Downloading " + pluginId + " from maven central");
            return downloadAndValidate(terminal, mavenUrl, tmpDir, false, isBatch);
        }

        // fall back to plain old URL
        if (pluginId.contains(":") == false) {
            // definitely not a valid url, so assume it is a plugin name
            List<String> plugins = checkMisspelledPlugin(pluginId);
            String msg = "Unknown plugin " + pluginId;
            if (plugins.isEmpty() == false) {
                msg += ", did you mean " + (plugins.size() == 1 ? "[" + plugins.get(0) + "]" : "any of " + plugins.toString()) + "?";
            }
            throw new UserException(ExitCodes.USAGE, msg);
        }
        terminal.println("-> Downloading " + URLDecoder.decode(pluginId, "UTF-8"));
        return downloadZip(terminal, pluginId, tmpDir, isBatch);
    }

    boolean isSnapshot() {
        return Build.CURRENT.isSnapshot();
    }

    /** Returns the url for an official opensearch plugin. */
    private String getOpenSearchUrl(
        final Terminal terminal,
        final Version version,
        final boolean isSnapshot,
        final String pluginId,
        final String platform
    ) throws IOException, UserException {
        final String baseUrl;
        if (isSnapshot == true) {
            baseUrl = String.format(
                Locale.ROOT,
                "https://artifacts.opensearch.org/snapshots/plugins/%s/%s",
                pluginId,
                Build.CURRENT.getQualifiedVersion()
            );
        } else {
            baseUrl = String.format(
                Locale.ROOT,
                "https://artifacts.opensearch.org/releases/plugins/%s/%s",
                pluginId,
                Build.CURRENT.getQualifiedVersion()
            );
        }
        final String platformUrl = String.format(
            Locale.ROOT,
            "%s/%s-%s-%s.zip",
            baseUrl,
            pluginId,
            platform,
            Build.CURRENT.getQualifiedVersion()
        );
        if (urlExists(terminal, platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, pluginId, Build.CURRENT.getQualifiedVersion());
    }

    /** Returns the url for an opensearch plugin in maven. */
    private String getMavenUrl(Terminal terminal, String[] coordinates, String platform) throws IOException {
        final String groupId = coordinates[0].replace(".", "/");
        final String artifactId = coordinates[1];
        final String version = coordinates[2];
        final String baseUrl = String.format(Locale.ROOT, "https://repo1.maven.org/maven2/%s/%s/%s", groupId, artifactId, version);
        final String platformUrl = String.format(Locale.ROOT, "%s/%s-%s-%s.zip", baseUrl, artifactId, platform, version);
        if (urlExists(terminal, platformUrl)) {
            return platformUrl;
        }
        return String.format(Locale.ROOT, "%s/%s-%s.zip", baseUrl, artifactId, version);
    }

    /**
     * Returns {@code true} if the given url exists, and {@code false} otherwise.
     * <p>
     * The given url must be {@code https} and existing means a {@code HEAD} request returns 200.
     */
    // pkg private for tests to manipulate
    @SuppressForbidden(reason = "Make HEAD request using URLConnection.connect()")
    boolean urlExists(Terminal terminal, String urlString) throws IOException {
        terminal.println(VERBOSE, "Checking if url exists: " + urlString);
        URL url = new URL(urlString);
        assert "https".equals(url.getProtocol()) : "Use of https protocol is required";
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        urlConnection.addRequestProperty("User-Agent", "opensearch-plugin-installer");
        urlConnection.setRequestMethod("HEAD");
        urlConnection.connect();
        return urlConnection.getResponseCode() == 200;
    }

    /** Returns all the official plugin names that look similar to pluginId. **/
    private List<String> checkMisspelledPlugin(String pluginId) {
        LevenshteinDistance ld = new LevenshteinDistance();
        List<Tuple<Float, String>> scoredKeys = new ArrayList<>();
        for (String officialPlugin : OFFICIAL_PLUGINS) {
            float distance = ld.getDistance(pluginId, officialPlugin);
            if (distance > 0.7f) {
                scoredKeys.add(new Tuple<>(distance, officialPlugin));
            }
        }
        CollectionUtil.timSort(scoredKeys, (a, b) -> b.v1().compareTo(a.v1()));
        return scoredKeys.stream().map((a) -> a.v2()).collect(Collectors.toList());
    }

    /** Downloads a zip from the url, into a temp file under the given temp dir. */
    // pkg private for tests
    @SuppressForbidden(reason = "We use getInputStream to download plugins")
    Path downloadZip(Terminal terminal, String urlString, Path tmpDir, boolean isBatch) throws IOException {
        terminal.println(VERBOSE, "Retrieving zip from " + urlString);
        URL url = new URL(urlString);
        Path zip = Files.createTempFile(tmpDir, null, ".zip");
        URLConnection urlConnection = url.openConnection();
        urlConnection.addRequestProperty("User-Agent", "opensearch-plugin-installer");
        try (
            InputStream in = isBatch
                ? urlConnection.getInputStream()
                : new TerminalProgressInputStream(urlConnection.getInputStream(), urlConnection.getContentLength(), terminal)
        ) {
            // must overwrite since creating the temp file above actually created the file
            Files.copy(in, zip, StandardCopyOption.REPLACE_EXISTING);
        }
        return zip;
    }

    /**
     * content length might be -1 for unknown and progress only makes sense if the content length is greater than 0
     */
    private class TerminalProgressInputStream extends ProgressInputStream {

        private final Terminal terminal;
        private int width = 50;
        private final boolean enabled;

        TerminalProgressInputStream(InputStream is, int expectedTotalSize, Terminal terminal) {
            super(is, expectedTotalSize);
            this.terminal = terminal;
            this.enabled = expectedTotalSize > 0;
        }

        @Override
        public void onProgress(int percent) {
            if (enabled) {
                int currentPosition = percent * width / 100;
                StringBuilder sb = new StringBuilder("\r[");
                sb.append(String.join("=", Collections.nCopies(currentPosition, "")));
                if (currentPosition > 0 && percent < 100) {
                    sb.append(">");
                }
                sb.append(String.join(" ", Collections.nCopies(width - currentPosition, "")));
                sb.append("] %s   ");
                if (percent == 100) {
                    sb.append("\n");
                }
                terminal.print(Terminal.Verbosity.NORMAL, String.format(Locale.ROOT, sb.toString(), percent + "%"));
            }
        }
    }

    @SuppressForbidden(reason = "URL#openStream")
    private InputStream urlOpenStream(final URL url) throws IOException {
        return url.openStream();
    }

    /**
     * Downloads a ZIP from the URL. This method also validates the downloaded plugin ZIP via the following means:
     * <ul>
     * <li>
     * For an official plugin we download the SHA-512 checksum and validate the integrity of the downloaded ZIP. We also download the
     * armored signature and validate the authenticity of the downloaded ZIP.
     * </li>
     * <li>
     * For a non-official plugin we download the SHA-512 checksum and fallback to the SHA-1 checksum and validate the integrity of the
     * downloaded ZIP.
     * </li>
     * </ul>
     *
     * @param terminal       a terminal to log messages to
     * @param urlString      the URL of the plugin ZIP
     * @param tmpDir         a temporary directory to write downloaded files to
     * @param officialPlugin true if the plugin is an official plugin
     * @param isBatch        true if the install is running in batch mode
     * @return the path to the downloaded plugin ZIP
     * @throws IOException   if an I/O exception occurs download or reading files and resources
     * @throws PGPException  if an exception occurs verifying the downloaded ZIP signature
     * @throws UserException if checksum validation fails
     */
    private Path downloadAndValidate(
        final Terminal terminal,
        final String urlString,
        final Path tmpDir,
        final boolean officialPlugin,
        boolean isBatch
    ) throws IOException, PGPException, UserException {
        Path zip = downloadZip(terminal, urlString, tmpDir, isBatch);
        pathsToDeleteOnShutdown.add(zip);
        String checksumUrlString = urlString + ".sha512";
        URL checksumUrl = openUrl(checksumUrlString);
        String digestAlgo = "SHA-512";
        if (checksumUrl == null && officialPlugin == false) {
            // fallback to sha1, until 7.0, but with warning
            terminal.println(
                "Warning: sha512 not found, falling back to sha1. This behavior is deprecated and will be removed in a "
                    + "future release. Please update the plugin to use a sha512 checksum."
            );
            checksumUrlString = urlString + ".sha1";
            checksumUrl = openUrl(checksumUrlString);
            digestAlgo = "SHA-1";
        }
        if (checksumUrl == null) {
            throw new UserException(ExitCodes.IO_ERROR, "Plugin checksum missing: " + checksumUrlString);
        }
        final String expectedChecksum;
        try (InputStream in = urlOpenStream(checksumUrl)) {
            /*
             * The supported format of the SHA-1 files is a single-line file containing the SHA-1. The supported format of the SHA-512 files
             * is a single-line file containing the SHA-512 and the filename, separated by two spaces. For SHA-1, we verify that the hash
             * matches, and that the file contains a single line. For SHA-512, we verify that the hash and the filename match, and that the
             * file contains a single line.
             */
            if (digestAlgo.equals("SHA-1")) {
                final BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                expectedChecksum = checksumReader.readLine();
                if (checksumReader.readLine() != null) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
            } else {
                final BufferedReader checksumReader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                final String checksumLine = checksumReader.readLine();
                final String[] fields = checksumLine.split(" {2}");
                if (officialPlugin && fields.length != 2 || officialPlugin == false && fields.length > 2) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
                expectedChecksum = fields[0];
                if (fields.length == 2) {
                    // checksum line contains filename as well
                    final String[] segments = URI.create(urlString).getPath().split("/");
                    final String expectedFile = segments[segments.length - 1];
                    if (fields[1].equals(expectedFile) == false) {
                        final String message = String.format(
                            Locale.ROOT,
                            "checksum file at [%s] is not for this plugin, expected [%s] but was [%s]",
                            checksumUrl,
                            expectedFile,
                            fields[1]
                        );
                        throw new UserException(ExitCodes.IO_ERROR, message);
                    }
                }
                if (checksumReader.readLine() != null) {
                    throw new UserException(ExitCodes.IO_ERROR, "Invalid checksum file at " + checksumUrl);
                }
            }
        }

        // read the bytes of the plugin zip in chunks to avoid out of memory errors
        try (InputStream zis = Files.newInputStream(zip)) {
            try {
                final MessageDigest digest = MessageDigest.getInstance(digestAlgo);
                final byte[] bytes = new byte[8192];
                int read;
                while ((read = zis.read(bytes)) != -1) {
                    assert read > 0 : read;
                    digest.update(bytes, 0, read);
                }
                final String actualChecksum = MessageDigests.toHexString(digest.digest());
                if (expectedChecksum.equals(actualChecksum) == false) {
                    throw new UserException(
                        ExitCodes.IO_ERROR,
                        digestAlgo + " mismatch, expected " + expectedChecksum + " but got " + actualChecksum
                    );
                }
            } catch (final NoSuchAlgorithmException e) {
                // this should never happen as we are using SHA-1 and SHA-512 here
                throw new AssertionError(e);
            }
        }

        if (officialPlugin) {
            verifySignature(zip, urlString);
        }

        return zip;
    }

    /**
     * Verify the signature of the downloaded plugin ZIP. The signature is obtained from the source of the downloaded plugin by appending
     * ".sig" to the URL. It is expected that the plugin is signed with the OpenSearch signing key with ID C2EE2AF6542C03B4.
     *
     * @param zip       the path to the downloaded plugin ZIP
     * @param urlString the URL source of the downloade plugin ZIP
     * @throws IOException  if an I/O exception occurs reading from various input streams
     * @throws PGPException if the PGP implementation throws an internal exception during verification
     */
    void verifySignature(final Path zip, final String urlString) throws IOException, PGPException {
        final String sigUrlString = urlString + ".sig";
        final URL sigUrl = openUrl(sigUrlString);
        try (
            // fin is a file stream over the downloaded plugin zip whose signature to verify
            InputStream fin = pluginZipInputStream(zip);
            // sin is a URL stream to the signature corresponding to the downloaded plugin zip
            InputStream sin = urlOpenStream(sigUrl);
            // ain is a input stream to the public key in ASCII-Armor format (RFC4880)
            InputStream ain = new ArmoredInputStream(getPublicKey())
        ) {
            final JcaPGPObjectFactory factory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(sin));
            final PGPSignature signature = ((PGPSignatureList) factory.nextObject()).get(0);

            // validate the signature has key ID matching our public key ID
            final String keyId = Long.toHexString(signature.getKeyID()).toUpperCase(Locale.ROOT);
            if (getPublicKeyId().equals(keyId) == false) {
                throw new IllegalStateException("key id [" + keyId + "] does not match expected key id [" + getPublicKeyId() + "]");
            }

            // compute the signature of the downloaded plugin zip
            final PGPPublicKeyRingCollection collection = new PGPPublicKeyRingCollection(ain, new JcaKeyFingerprintCalculator());
            final PGPPublicKey key = collection.getPublicKey(signature.getKeyID());
            signature.init(new JcaPGPContentVerifierBuilderProvider().setProvider(new BouncyCastleFipsProvider()), key);
            final byte[] buffer = new byte[1024];
            int read;
            while ((read = fin.read(buffer)) != -1) {
                signature.update(buffer, 0, read);
            }

            // finally we verify the signature of the downloaded plugin zip matches the expected signature
            if (signature.verify() == false) {
                throw new IllegalStateException("signature verification for [" + urlString + "] failed");
            }
        }
    }

    /**
     * An input stream to the raw bytes of the plugin ZIP.
     *
     * @param zip the path to the downloaded plugin ZIP
     * @return an input stream to the raw bytes of the plugin ZIP.
     * @throws IOException if an I/O exception occurs preparing the input stream
     */
    InputStream pluginZipInputStream(final Path zip) throws IOException {
        return Files.newInputStream(zip);
    }

    /**
     * Return the public key ID of the signing key that is expected to have signed the official plugin.
     *
     * @return the public key ID
     */
    String getPublicKeyId() {
        return "C2EE2AF6542C03B4";
    }

    /**
     * An input stream to the public key of the signing key.
     *
     * @return an input stream to the public key
     */
    InputStream getPublicKey() {
        return InstallPluginCommand.class.getResourceAsStream("/public_key.sig");
    }

    /**
     * Creates a URL and opens a connection.
     * If the URL returns a 404, {@code null} is returned, otherwise the open URL opject is returned.
     */
    // pkg private for tests
    URL openUrl(String urlString) throws IOException {
        URL checksumUrl = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection) checksumUrl.openConnection();
        if (connection.getResponseCode() == 404) {
            return null;
        }
        return checksumUrl;
    }

    private Path unzip(Path zip, Path pluginsDir) throws IOException, UserException {
        // unzip plugin to a staging temp dir

        final Path target = stagingDirectory(pluginsDir);
        pathsToDeleteOnShutdown.add(target);

        try (ZipFile zipFile = new ZipFile(zip, "UTF8", true, false)) {
            final Enumeration<? extends ZipArchiveEntry> entries = zipFile.getEntries();
            ZipArchiveEntry entry;
            byte[] buffer = new byte[8192];
            while (entries.hasMoreElements()) {
                entry = entries.nextElement();
                if (entry.getName().startsWith("opensearch/")) {
                    throw new UserException(
                        PLUGIN_MALFORMED,
                        "This plugin was built with an older plugin structure."
                            + " Contact the plugin author to remove the intermediate \"opensearch\" directory within the plugin zip."
                    );
                }
                Path targetFile = target.resolve(entry.getName());

                // Using the entry name as a path can result in an entry outside of the plugin dir,
                // either if the name starts with the root of the filesystem, or it is a relative
                // entry like ../whatever. This check attempts to identify both cases by first
                // normalizing the path (which removes foo/..) and ensuring the normalized entry
                // is still rooted with the target plugin directory.
                if (targetFile.normalize().startsWith(target) == false) {
                    throw new UserException(
                        PLUGIN_MALFORMED,
                        "Zip contains entry name '" + entry.getName() + "' resolving outside of plugin directory"
                    );
                }

                // be on the safe side: do not rely on that directories are always extracted
                // before their children (although this makes sense, but is it guaranteed?)
                if (!Files.isSymbolicLink(targetFile.getParent())) {
                    Files.createDirectories(targetFile.getParent());
                }
                if (entry.isDirectory() == false) {
                    // streams will be auto-closed with try-with-resources
                    try (OutputStream out = Files.newOutputStream(targetFile); InputStream input = zipFile.getInputStream(entry)) {
                        input.transferTo(out);
                    }
                }
            }
        } catch (UserException e) {
            IOUtils.rm(target);
            throw e;
        }
        Files.delete(zip);
        return target;
    }

    private Path stagingDirectory(Path pluginsDir) throws IOException {
        try {
            return Files.createTempDirectory(pluginsDir, ".installing-", PosixFilePermissions.asFileAttribute(PLUGIN_DIR_PERMS));
        } catch (IllegalArgumentException e) {
            // Jimfs throws an IAE where it should throw an UOE
            // remove when google/jimfs#30 is integrated into Jimfs
            // and the Jimfs test dependency is upgraded to include
            // this pull request
            final StackTraceElement[] elements = e.getStackTrace();
            if (elements.length >= 1
                && elements[0].getClassName().equals("com.google.common.jimfs.AttributeService")
                && elements[0].getMethodName().equals("setAttributeInternal")) {
                return stagingDirectoryWithoutPosixPermissions(pluginsDir);
            } else {
                throw e;
            }
        } catch (UnsupportedOperationException e) {
            return stagingDirectoryWithoutPosixPermissions(pluginsDir);
        }
    }

    private Path stagingDirectoryWithoutPosixPermissions(Path pluginsDir) throws IOException {
        return Files.createTempDirectory(pluginsDir, ".installing-");
    }

    // checking for existing version of the plugin
    private void verifyPluginName(Path pluginPath, String pluginName) throws UserException, IOException {
        // don't let user install plugin conflicting with module...
        // they might be unavoidably in maven central and are packaged up the same way)
        if (MODULES.contains(pluginName)) {
            throw new UserException(ExitCodes.USAGE, "plugin '" + pluginName + "' cannot be installed as a plugin, it is a system module");
        }

        // scan all the installed plugins to see if the plugin being installed already exists
        // either with the plugin name or a custom folder name
        Path destination = PluginHelper.verifyIfPluginExists(pluginPath, pluginName);
        if (Files.exists(destination)) {
            final String message = String.format(
                Locale.ROOT,
                "plugin directory [%s] already exists; if you need to update the plugin, " + "uninstall it first using command 'remove %s'",
                destination,
                pluginName
            );
            throw new UserException(PLUGIN_EXISTS, message);
        }
    }

    /** Load information about the plugin, and verify it can be installed with no errors. */
    private PluginInfo loadPluginInfo(Terminal terminal, Path pluginRoot, Environment env) throws Exception {
        final PluginInfo info = PluginInfo.readFromProperties(pluginRoot);
        if (info.hasNativeController()) {
            throw new IllegalStateException("plugins can not have native controllers");
        }
        PluginsService.verifyCompatibility(info);

        // checking for existing version of the plugin
        verifyPluginName(env.pluginsDir(), info.getName());

        PluginsService.checkForFailedPluginRemovals(env.pluginsDir());

        terminal.println(VERBOSE, info.toString());

        // check for jar hell before any copying
        jarHellCheck(info, pluginRoot, env.pluginsDir(), env.modulesDir());

        return info;
    }

    private static final String LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR;

    static {
        LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR = String.format(Locale.ROOT, ".+%1$slib%1$stools%1$splugin-cli%1$s[^%1$s]+\\.jar", "(/|\\\\)");
    }

    /** check a candidate plugin for jar hell before installing it */
    void jarHellCheck(PluginInfo candidateInfo, Path candidateDir, Path pluginsDir, Path modulesDir) throws Exception {
        // create list of current jars in classpath
        final Set<URL> classpath = JarHell.parseClassPath().stream().filter(url -> {
            try {
                return url.toURI().getPath().matches(LIB_TOOLS_PLUGIN_CLI_CLASSPATH_JAR) == false;
            } catch (final URISyntaxException e) {
                throw new AssertionError(e);
            }
        }).collect(Collectors.toSet());

        // read existing bundles. this does some checks on the installation too.
        Set<PluginsService.Bundle> bundles = new HashSet<>(PluginsService.getPluginBundles(pluginsDir));
        bundles.addAll(PluginsService.getModuleBundles(modulesDir));
        bundles.add(new PluginsService.Bundle(candidateInfo, candidateDir));
        List<PluginsService.Bundle> sortedBundles = PluginsService.sortBundles(bundles);

        // check jarhell of all plugins so we know this plugin and anything depending on it are ok together
        // TODO: optimize to skip any bundles not connected to the candidate plugin?
        Map<String, Set<URL>> transitiveUrls = new HashMap<>();
        for (PluginsService.Bundle bundle : sortedBundles) {
            PluginsService.checkBundleJarHell(classpath, bundle, transitiveUrls);
        }

        // TODO: no jars should be an error
        // TODO: verify the classname exists in one of the jars!
    }

    /**
     * Installs the plugin from {@code tmpRoot} into the plugins dir.
     * If the plugin has a bin dir and/or a config dir, those are moved.
     */
    private PluginInfo installPlugin(Terminal terminal, boolean isBatch, Path tmpRoot, Environment env, List<Path> deleteOnFailure)
        throws Exception {
        final PluginInfo info = loadPluginInfo(terminal, tmpRoot, env);
        // read optional security policy (extra permissions), if it exists, confirm or warn the user
        Path policy = tmpRoot.resolve(PluginInfo.OPENSEARCH_PLUGIN_POLICY);
        final Set<String> permissions;
        if (Files.exists(policy)) {
            permissions = PluginSecurity.parsePermissions(policy, env.tmpDir());
        } else {
            permissions = Collections.emptySet();
        }
        PluginSecurity.confirmPolicyExceptions(terminal, permissions, isBatch);

        String targetFolderName = info.getTargetFolderName();
        final Path destination = env.pluginsDir().resolve(targetFolderName);
        deleteOnFailure.add(destination);

        installPluginSupportFiles(
            info,
            tmpRoot,
            env.binDir().resolve(targetFolderName),
            env.configDir().resolve(targetFolderName),
            deleteOnFailure
        );
        movePlugin(tmpRoot, destination);
        return info;
    }

    /** Moves bin and config directories from the plugin if they exist */
    private void installPluginSupportFiles(PluginInfo info, Path tmpRoot, Path destBinDir, Path destConfigDir, List<Path> deleteOnFailure)
        throws Exception {
        Path tmpBinDir = tmpRoot.resolve("bin");
        if (Files.exists(tmpBinDir)) {
            deleteOnFailure.add(destBinDir);
            installBin(info, tmpBinDir, destBinDir);
        }

        Path tmpConfigDir = tmpRoot.resolve("config");
        if (Files.exists(tmpConfigDir)) {
            // some files may already exist, and we don't remove plugin config files on plugin removal,
            // so any installed config files are left on failure too
            installConfig(info, tmpConfigDir, destConfigDir);
        }
    }

    /** Moves the plugin directory into its final destination. **/
    private void movePlugin(Path tmpRoot, Path destination) throws IOException {
        Files.move(tmpRoot, destination, StandardCopyOption.ATOMIC_MOVE);
        Files.walkFileTree(destination, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                final String parentDirName = file.getParent().getFileName().toString();
                if ("bin".equals(parentDirName)
                    // "MacOS" is an alternative to "bin" on macOS
                    || (Constants.MAC_OS_X && "MacOS".equals(parentDirName))) {
                    setFileAttributes(file, BIN_FILES_PERMS);
                } else {
                    setFileAttributes(file, PLUGIN_FILES_PERMS);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                setFileAttributes(dir, PLUGIN_DIR_PERMS);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** Copies the files from {@code tmpBinDir} into {@code destBinDir}, along with permissions from dest dirs parent. */
    private void installBin(PluginInfo info, Path tmpBinDir, Path destBinDir) throws Exception {
        if (Files.isDirectory(tmpBinDir) == false) {
            throw new UserException(PLUGIN_MALFORMED, "bin in plugin " + info.getName() + " is not a directory");
        }
        Files.createDirectories(destBinDir);
        setFileAttributes(destBinDir, BIN_DIR_PERMS);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpBinDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new UserException(
                        PLUGIN_MALFORMED,
                        "Directories not allowed in bin dir " + "for plugin " + info.getName() + ", found " + srcFile.getFileName()
                    );
                }

                Path destFile = destBinDir.resolve(tmpBinDir.relativize(srcFile));
                Files.copy(srcFile, destFile);
                setFileAttributes(destFile, BIN_FILES_PERMS);
            }
        }
        IOUtils.rm(tmpBinDir); // clean up what we just copied
    }

    /**
     * Copies the files from {@code tmpConfigDir} into {@code destConfigDir}.
     * Any files existing in both the source and destination will be skipped.
     */
    private void installConfig(PluginInfo info, Path tmpConfigDir, Path destConfigDir) throws Exception {
        if (Files.isDirectory(tmpConfigDir) == false) {
            throw new UserException(PLUGIN_MALFORMED, "config in plugin " + info.getName() + " is not a directory");
        }

        Files.createDirectories(destConfigDir);
        setFileAttributes(destConfigDir, CONFIG_DIR_PERMS);
        final PosixFileAttributeView destConfigDirAttributesView = Files.getFileAttributeView(
            destConfigDir.getParent(),
            PosixFileAttributeView.class
        );
        final PosixFileAttributes destConfigDirAttributes = destConfigDirAttributesView != null
            ? destConfigDirAttributesView.readAttributes()
            : null;
        if (destConfigDirAttributes != null) {
            setOwnerGroup(destConfigDir, destConfigDirAttributes);
        }

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(tmpConfigDir)) {
            for (Path srcFile : stream) {
                if (Files.isDirectory(srcFile)) {
                    throw new UserException(PLUGIN_MALFORMED, "Directories not allowed in config dir for plugin " + info.getName());
                }

                Path destFile = destConfigDir.resolve(tmpConfigDir.relativize(srcFile));
                if (Files.exists(destFile) == false) {
                    Files.copy(srcFile, destFile);
                    setFileAttributes(destFile, CONFIG_FILES_PERMS);
                    if (destConfigDirAttributes != null) {
                        setOwnerGroup(destFile, destConfigDirAttributes);
                    }
                }
            }
        }
        IOUtils.rm(tmpConfigDir); // clean up what we just copied
    }

    private static void setOwnerGroup(final Path path, final PosixFileAttributes attributes) throws IOException {
        Objects.requireNonNull(attributes);
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        assert fileAttributeView != null;
        fileAttributeView.setOwner(attributes.owner());
        fileAttributeView.setGroup(attributes.group());
    }

    /**
     * Sets the attributes for a path iff posix attributes are supported
     */
    private static void setFileAttributes(final Path path, final Set<PosixFilePermission> permissions) throws IOException {
        PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
        if (fileAttributeView != null) {
            Files.setPosixFilePermissions(path, permissions);
        }
    }

    private final List<Path> pathsToDeleteOnShutdown = new ArrayList<>();

    @Override
    public void close() throws IOException {
        IOUtils.rm(pathsToDeleteOnShutdown.toArray(new Path[0]));
    }

}
