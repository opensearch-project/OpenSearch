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

package org.opensearch.packaging.test;

import org.opensearch.packaging.util.Distribution;
import org.opensearch.packaging.util.Docker;
import org.opensearch.packaging.util.FileUtils;
import org.opensearch.packaging.util.Installation;
import org.opensearch.packaging.util.Packages;
import org.opensearch.packaging.util.Platforms;
import org.opensearch.packaging.util.ServerUtils;
import org.opensearch.packaging.util.Shell;
import org.junit.Ignore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.packaging.util.Archives.ARCHIVE_OWNER;
import static org.opensearch.packaging.util.Archives.installArchive;
import static org.opensearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.opensearch.packaging.util.Docker.assertPermissionsAndOwnership;
import static org.opensearch.packaging.util.Docker.runContainer;
import static org.opensearch.packaging.util.Docker.runContainerExpectingFailure;
import static org.opensearch.packaging.util.Docker.waitForOpenSearch;
import static org.opensearch.packaging.util.Docker.waitForPathToExist;
import static org.opensearch.packaging.util.FileMatcher.Fileness.File;
import static org.opensearch.packaging.util.FileMatcher.file;
import static org.opensearch.packaging.util.FileMatcher.p600;
import static org.opensearch.packaging.util.FileMatcher.p660;
import static org.opensearch.packaging.util.FileUtils.rm;
import static org.opensearch.packaging.util.Packages.assertInstalled;
import static org.opensearch.packaging.util.Packages.assertRemoved;
import static org.opensearch.packaging.util.Packages.installPackage;
import static org.opensearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.startsWith;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;

public class KeystoreManagementTests extends PackagingTestCase {

    public static final String ERROR_INCORRECT_PASSWORD = "Provided keystore password was incorrect";
    public static final String ERROR_CORRUPTED_KEYSTORE = "Keystore has been corrupted or tampered with";
    public static final String ERROR_KEYSTORE_NOT_PASSWORD_PROTECTED = "ERROR: Keystore is not password-protected";
    public static final String ERROR_KEYSTORE_NOT_FOUND = "ERROR: OpenSearch keystore not found";

    /** Test initial archive state */
    public void test10InstallArchiveDistribution() throws Exception {
        assumeTrue(distribution().isArchive());

        installation = installArchive(sh, distribution);
        verifyArchiveInstallation(installation, distribution());

        final Installation.Executables bin = installation.executables();
        Shell.Result r = sh.runIgnoreExitCode(bin.keystoreTool.toString() + " has-passwd");
        assertFalse("has-passwd should fail", r.isSuccess());
        assertThat("has-passwd should indicate missing keystore", r.stderr, containsString(ERROR_KEYSTORE_NOT_FOUND));
    }

    /** Test initial package state */
    public void test11InstallPackageDistribution() throws Exception {
        assumeTrue(distribution().isPackage());

        assertRemoved(distribution);
        installation = installPackage(sh, distribution);
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);

        final Installation.Executables bin = installation.executables();
        Shell.Result r = sh.runIgnoreExitCode(bin.keystoreTool.toString() + " has-passwd");
        assertFalse("has-passwd should fail", r.isSuccess());
        assertThat("has-passwd should indicate unprotected keystore", r.stderr, containsString(ERROR_KEYSTORE_NOT_PASSWORD_PROTECTED));
        Shell.Result r2 = bin.keystoreTool.run("list");
        assertThat(r2.stdout, containsString("keystore.seed"));
    }

    /** Test initial Docker state */
    public void test12InstallDockerDistribution() throws Exception {
        assumeTrue(distribution().isDocker());

        installation = Docker.runContainer(distribution());

        try {
            waitForPathToExist(installation.config("opensearch.keystore"));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        final Installation.Executables bin = installation.executables();
        Shell.Result r = sh.runIgnoreExitCode(bin.keystoreTool.toString() + " has-passwd");
        assertFalse("has-passwd should fail", r.isSuccess());
        assertThat("has-passwd should indicate unprotected keystore", r.stdout, containsString(ERROR_KEYSTORE_NOT_PASSWORD_PROTECTED));
        Shell.Result r2 = bin.keystoreTool.run("list");
        assertThat(r2.stdout, containsString("keystore.seed"));
    }

    public void test20CreateKeystoreManually() throws Exception {
        rmKeystoreIfExists();
        createKeystore(null);

        final Installation.Executables bin = installation.executables();
        verifyKeystorePermissions();

        Shell.Result r = bin.keystoreTool.run("list");
        assertThat(r.stdout, containsString("keystore.seed"));
    }

    public void test30AutoCreateKeystore() throws Exception {
        assumeTrue("Packages and docker are installed with a keystore file", distribution.isArchive());
        rmKeystoreIfExists();

        startOpenSearch();
        stopOpenSearch();

        Platforms.onWindows(() -> sh.chown(installation.config("opensearch.keystore")));

        verifyKeystorePermissions();

        final Installation.Executables bin = installation.executables();
        Shell.Result r = bin.keystoreTool.run("list");
        assertThat(r.stdout, containsString("keystore.seed"));
    }

    public void test40KeystorePasswordOnStandardInput() throws Exception {
        assumeTrue("packages will use systemd, which doesn't handle stdin", distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        String password = "^|<>\\&exit"; // code insertion on Windows if special characters are not escaped

        rmKeystoreIfExists();
        createKeystore(password);

        assertPasswordProtectedKeystore();

        awaitOpenSearchStartup(runOpenSearchStartCommand(password, true, false));
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();
    }

    public void test41WrongKeystorePasswordOnStandardInput() throws Exception {
        assumeTrue("packages will use systemd, which doesn't handle stdin", distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        assertPasswordProtectedKeystore();

        Shell.Result result = runOpenSearchStartCommand("wrong", false, false);
        assertOpenSearchFailure(result, Arrays.asList(ERROR_INCORRECT_PASSWORD, ERROR_CORRUPTED_KEYSTORE), null);
    }

    /**
     * This test simulates a user starting OpenSearch on the command line without daemonizing
     */
    public void test42KeystorePasswordOnTtyRunningInForeground() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        assumeTrue("packages will use systemd, which doesn't handle stdin", distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        String password = "keystorepass";

        rmKeystoreIfExists();
        createKeystore(password);

        assertPasswordProtectedKeystore();

        awaitOpenSearchStartup(runOpenSearchStartCommand(password, false, true));
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();
    }

    @Ignore // awaits fix: https://github.com/elastic/elasticsearch/issues/49340
    public void test43KeystorePasswordOnTtyDaemonized() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        assumeTrue("packages will use systemd, which doesn't handle stdin", distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        String password = "keystorepass";

        rmKeystoreIfExists();
        createKeystore(password);

        assertPasswordProtectedKeystore();

        awaitOpenSearchStartup(runOpenSearchStartCommand(password, true, true));
        ServerUtils.runOpenSearchTests();
        stopOpenSearch();
    }

    public void test44WrongKeystorePasswordOnTty() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        assumeTrue("packages will use systemd, which doesn't handle stdin", distribution.isArchive());
        assumeThat(installation, is(notNullValue()));

        assertPasswordProtectedKeystore();

        // daemonization shouldn't matter for this test
        boolean daemonize = randomBoolean();
        Shell.Result result = runOpenSearchStartCommand("wrong", daemonize, true);
        // error will be on stdout for "expect"
        assertThat(result.stdout, anyOf(containsString(ERROR_INCORRECT_PASSWORD), containsString(ERROR_CORRUPTED_KEYSTORE)));
    }

    /**
     * If we have an encrypted keystore, we shouldn't require a password to
     * view help information.
     */
    public void test45EncryptedKeystoreAllowsHelpMessage() throws Exception {
        assumeTrue("users call opensearch directly in archive case", distribution.isArchive());

        String password = "keystorepass";

        rmKeystoreIfExists();
        createKeystore(password);

        assertPasswordProtectedKeystore();
        Shell.Result r = installation.executables().opensearch.run("--help");
        assertThat(r.stdout, startsWith("Starts OpenSearch"));
    }

    public void test50KeystorePasswordFromFile() throws Exception {
        assumeTrue("only for systemd", Platforms.isSystemd() && distribution().isPackage());
        String password = "!@#$%^&*()|\\<>/?";
        Path esKeystorePassphraseFile = installation.config.resolve("eks");

        rmKeystoreIfExists();
        createKeystore(password);

        assertPasswordProtectedKeystore();

        try {
            sh.run("sudo systemctl set-environment OPENSEARCH_KEYSTORE_PASSPHRASE_FILE=" + esKeystorePassphraseFile);

            Files.createFile(esKeystorePassphraseFile);
            Files.write(esKeystorePassphraseFile, singletonList(password));

            startOpenSearch();
            ServerUtils.runOpenSearchTests();
            stopOpenSearch();
        } finally {
            sh.run("sudo systemctl unset-environment OPENSEARCH_KEYSTORE_PASSPHRASE_FILE");
        }
    }

    public void test51WrongKeystorePasswordFromFile() throws Exception {
        assumeTrue("only for systemd", Platforms.isSystemd() && distribution().isPackage());
        Path esKeystorePassphraseFile = installation.config.resolve("eks");

        assertPasswordProtectedKeystore();

        try {
            sh.run("sudo systemctl set-environment OPENSEARCH_KEYSTORE_PASSPHRASE_FILE=" + esKeystorePassphraseFile);

            if (Files.exists(esKeystorePassphraseFile)) {
                rm(esKeystorePassphraseFile);
            }

            Files.createFile(esKeystorePassphraseFile);
            Files.write(esKeystorePassphraseFile, singletonList("wrongpassword"));

            Packages.JournaldWrapper journaldWrapper = new Packages.JournaldWrapper(sh);
            Shell.Result result = runOpenSearchStartCommand(null, false, false);
            assertOpenSearchFailure(result, Arrays.asList(ERROR_INCORRECT_PASSWORD, ERROR_CORRUPTED_KEYSTORE), journaldWrapper);
        } finally {
            sh.run("sudo systemctl unset-environment OPENSEARCH_KEYSTORE_PASSPHRASE_FILE");
        }
    }

    /**
     * Check that we can mount a password-protected keystore to a docker image
     * and provide a password via an environment variable.
     */
    public void test60DockerEnvironmentVariablePassword() throws Exception {
        assumeTrue(distribution().isDocker());
        String password = "password";
        Path dockerKeystore = installation.config("opensearch.keystore");

        Path localKeystoreFile = getKeystoreFileFromDockerContainer(password, dockerKeystore);

        // restart ES with password and mounted keystore
        Map<Path, Path> volumes = new HashMap<>();
        volumes.put(localKeystoreFile, dockerKeystore);
        Map<String, String> envVars = new HashMap<>();
        envVars.put("KEYSTORE_PASSWORD", password);
        runContainer(distribution(), volumes, envVars);
        waitForOpenSearch(installation);
        ServerUtils.runOpenSearchTests();
    }

    /**
     * Check that we can mount a password-protected keystore to a docker image
     * and provide a password via a file, pointed at from an environment variable.
     */
    public void test61DockerEnvironmentVariablePasswordFromFile() throws Exception {
        assumeTrue(distribution().isDocker());

        Path tempDir = null;
        try {
            tempDir = createTempDir(DockerTests.class.getSimpleName());

            String password = "password";
            String passwordFilename = "password.txt";
            Files.write(tempDir.resolve(passwordFilename), singletonList(password));
            Files.setPosixFilePermissions(tempDir.resolve(passwordFilename), p600);

            Path dockerKeystore = installation.config("opensearch.keystore");

            Path localKeystoreFile = getKeystoreFileFromDockerContainer(password, dockerKeystore);

            // restart ES with password and mounted keystore
            Map<Path, Path> volumes = new HashMap<>();
            volumes.put(localKeystoreFile, dockerKeystore);
            volumes.put(tempDir, Paths.get("/run/secrets"));

            Map<String, String> envVars = new HashMap<>();
            envVars.put("KEYSTORE_PASSWORD_FILE", "/run/secrets/" + passwordFilename);

            runContainer(distribution(), volumes, envVars);

            waitForOpenSearch(installation);
            ServerUtils.runOpenSearchTests();
        } finally {
            if (tempDir != null) {
                rm(tempDir);
            }
        }
    }

    /**
     * Check that if we provide the wrong password for a mounted and password-protected
     * keystore, OpenSearch doesn't start.
     */
    public void test62DockerEnvironmentVariableBadPassword() throws Exception {
        assumeTrue(distribution().isDocker());
        String password = "password";
        Path dockerKeystore = installation.config("opensearch.keystore");

        Path localKeystoreFile = getKeystoreFileFromDockerContainer(password, dockerKeystore);

        // restart ES with password and mounted keystore
        Map<Path, Path> volumes = new HashMap<>();
        volumes.put(localKeystoreFile, dockerKeystore);
        Map<String, String> envVars = new HashMap<>();
        envVars.put("KEYSTORE_PASSWORD", "wrong");
        Shell.Result r = runContainerExpectingFailure(distribution(), volumes, envVars);
        assertThat(r.stderr, containsString(ERROR_INCORRECT_PASSWORD));
    }

    /**
     * In the Docker context, it's a little bit tricky to get a password-protected
     * keystore. All of the utilities we'd want to use are on the Docker image.
     * This method mounts a temporary directory to a Docker container, password-protects
     * the keystore, and then returns the path of the file that appears in the
     * mounted directory (now accessible from the local filesystem).
     */
    private Path getKeystoreFileFromDockerContainer(String password, Path dockerKeystore) throws IOException {
        // Mount a temporary directory for copying the keystore
        Path dockerTemp = Paths.get("/usr/tmp/keystore-tmp");
        Path tempDirectory = createTempDir(KeystoreManagementTests.class.getSimpleName());
        Map<Path, Path> volumes = new HashMap<>();
        volumes.put(tempDirectory, dockerTemp);

        // It's very tricky to properly quote a pipeline that you're passing to
        // a docker exec command, so we're just going to put a small script in the
        // temp folder.
        List<String> setPasswordScript = new ArrayList<>();
        setPasswordScript.add("echo \"" + password);
        setPasswordScript.add(password);
        setPasswordScript.add("\" | " + installation.executables().keystoreTool.toString() + " passwd");

        Files.write(tempDirectory.resolve("set-pass.sh"), setPasswordScript);

        runContainer(distribution(), volumes, null);
        try {
            waitForPathToExist(dockerTemp);
            waitForPathToExist(dockerKeystore);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        // We need a local shell to put the correct permissions on our mounted directory.
        Shell localShell = new Shell();
        localShell.run("docker exec --tty " + Docker.getContainerId() + " chown opensearch:root " + dockerTemp);
        localShell.run("docker exec --tty " + Docker.getContainerId() + " chown opensearch:root " + dockerTemp.resolve("set-pass.sh"));

        sh.run("bash " + dockerTemp.resolve("set-pass.sh"));

        // copy keystore to temp file to make it available to docker host
        sh.run("cp " + dockerKeystore + " " + dockerTemp);
        return tempDirectory.resolve("opensearch.keystore");
    }

    /** Create a keystore. Provide a password to password-protect it, otherwise use null */
    private void createKeystore(String password) throws Exception {
        Path keystore = installation.config("opensearch.keystore");
        final Installation.Executables bin = installation.executables();
        bin.keystoreTool.run("create");

        // this is a hack around the fact that we can't run a command in the same session as the same user but not as administrator.
        // the keystore ends up being owned by the Administrators group, so we manually set it to be owned by the vagrant user here.
        // from the server's perspective the permissions aren't really different, this is just to reflect what we'd expect in the tests.
        // when we run these commands as a role user we won't have to do this
        Platforms.onWindows(() -> sh.chown(keystore));

        if (distribution().isDocker()) {
            try {
                waitForPathToExist(keystore);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (password != null) {
            setKeystorePassword(password);
        }
    }

    private void rmKeystoreIfExists() {
        Path keystore = installation.config("opensearch.keystore");
        if (distribution().isDocker()) {
            try {
                waitForPathToExist(keystore);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Move the auto-created one out of the way, or else the CLI prompts asks us to confirm
            sh.run("rm " + keystore);
        } else {
            if (Files.exists(keystore)) {
                FileUtils.rm(keystore);
            }
        }
    }

    private void setKeystorePassword(String password) throws Exception {
        final Installation.Executables bin = installation.executables();

        // set the password by passing it to stdin twice
        Platforms.onLinux(() -> bin.keystoreTool.run("passwd", password + "\n" + password + "\n"));

        Platforms.onWindows(
            () -> sh.run("Invoke-Command -ScriptBlock {echo '" + password + "'; echo '" + password + "'} | " + bin.keystoreTool + " passwd")
        );
    }

    private void assertPasswordProtectedKeystore() {
        Shell.Result r = installation.executables().keystoreTool.run("has-passwd");
        assertThat("keystore should be password protected", r.exitCode, is(0));
    }

    private void verifyKeystorePermissions() {
        Path keystore = installation.config("opensearch.keystore");
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                assertThat(keystore, file(File, ARCHIVE_OWNER, ARCHIVE_OWNER, p660));
                break;
            case DEB:
            case RPM:
                assertThat(keystore, file(File, "root", "opensearch", p660));
                break;
            case DOCKER:
                assertPermissionsAndOwnership(keystore, p660);
                break;
            default:
                throw new IllegalStateException("Unknown OpenSearch packaging type.");
        }
    }
}
