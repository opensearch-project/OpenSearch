/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle.precommit;

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link LicenseHeadersTask}, in particular that a license registered through
 * {@link LicenseHeadersTask#additionalLicense} is actually applied. That path builds a modified copy
 * of {@code rat-config.xml} at execution time, so it is easy to break without noticing.
 */
public class LicenseHeadersTaskTests extends GradleUnitTestCase {

    /** A header that matches none of the licenses defined in rat-config.xml. */
    private static final String CUSTOM_HEADER = "/*\n * Licensed under the Totally Bespoke License v3.\n */\n";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testCustomLicenseIsAccepted() throws IOException {
        LicenseHeadersTask task = taskFor(sourceFileWithHeader(CUSTOM_HEADER));
        task.additionalLicense("BSPK ", "Bespoke", "Totally Bespoke License v3");

        // Would throw if the generated configuration failed to load or the license did not match.
        task.checkLicenseHeaders();

        assertThat(reportOf(task), containsString("BSPK"));
    }

    /**
     * The counterpart to {@link #testCustomLicenseIsAccepted}: without the registration the very
     * same file must be rejected, otherwise that test would pass even if the header were being
     * approved for some unrelated reason.
     */
    public void testUnregisteredLicenseIsRejected() throws IOException {
        LicenseHeadersTask task = taskFor(sourceFileWithHeader(CUSTOM_HEADER));

        GradleException e = expectThrows(GradleException.class, task::checkLicenseHeaders);
        assertThat(e.getMessage(), containsString("License header problems were found"));
    }

    /** The standard OpenSearch header is approved by the shipped configuration. */
    public void testApprovedHeaderIsAccepted() throws IOException {
        LicenseHeadersTask task = taskFor(sourceFileWithHeader("/*\n * SPDX-License-Identifier: Apache-2.0\n */\n"));

        task.checkLicenseHeaders();
    }

    /** Registering several licenses must not clobber the earlier ones. */
    public void testMultipleCustomLicensesAreAllApplied() throws IOException {
        File srcDir = sourceFileWithHeader(CUSTOM_HEADER);
        Files.writeString(new File(srcDir, "Second.java").toPath(), "/*\n * The Other License.\n */\nclass Second {}\n");

        LicenseHeadersTask task = taskFor(srcDir);
        task.additionalLicense("BSPK ", "Bespoke", "Totally Bespoke License v3");
        task.additionalLicense("OTHER", "Other", "The Other License");

        task.checkLicenseHeaders();

        String report = reportOf(task);
        assertThat(report, containsString("BSPK"));
        assertThat(report, containsString("OTHER"));
    }

    public void testCategoryMustBeFiveCharacters() throws IOException {
        LicenseHeadersTask task = taskFor(sourceFileWithHeader(CUSTOM_HEADER));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> task.additionalLicense("TOOLONG", "Bespoke", "pattern")
        );
        assertThat(e.getMessage(), containsString("must be exactly 5 characters"));
    }

    /**
     * A category that happens to collide with one already defined in rat-config.xml still works: the
     * duplicate family definition is ignored and the license is registered against the existing,
     * already approved family.
     */
    public void testCategoryCollidingWithShippedFamilyStillMatches() throws IOException {
        LicenseHeadersTask task = taskFor(sourceFileWithHeader("/*\n * Colliding Marker Text.\n */\n"));
        task.additionalLicense("SPDX ", "Collide", "Colliding Marker Text");

        task.checkLicenseHeaders();

        assertThat(reportOf(task), containsString("Collide"));
    }

    /** Writes a java file carrying the given header and returns the source directory. */
    private File sourceFileWithHeader(String header) throws IOException {
        File srcDir = new File(temporaryFolder.getRoot(), "src/main/java");
        Files.createDirectories(srcDir.toPath());
        Files.writeString(new File(srcDir, "Example.java").toPath(), header + "class Example {}\n");
        return srcDir;
    }

    private LicenseHeadersTask taskFor(File srcDir) {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.getRoot()).build();
        project.getPlugins().apply(JavaPlugin.class);
        assertTrue("source directory should exist", srcDir.isDirectory());

        TaskProvider<LicenseHeadersTask> provider = project.getTasks().register("licenseHeaders", LicenseHeadersTask.class);
        return provider.get();
    }

    private String reportOf(LicenseHeadersTask task) throws IOException {
        return Files.readString(task.getReportFile().toPath());
    }
}
