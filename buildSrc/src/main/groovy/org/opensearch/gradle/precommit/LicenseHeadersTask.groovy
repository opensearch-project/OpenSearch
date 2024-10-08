/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
package org.opensearch.gradle.precommit

import org.apache.rat.Defaults
import org.apache.rat.ReportConfiguration
import org.apache.rat.Reporter
import org.apache.rat.analysis.matchers.SimpleTextMatcher
import org.apache.rat.anttasks.License
import org.apache.rat.anttasks.ResourceCollectionContainer
import org.apache.rat.license.ILicense
import org.apache.rat.license.ILicenseFamily
import org.apache.rat.license.ILicenseFamilyBuilder
import org.apache.rat.utils.DefaultLog
import org.apache.tools.ant.types.resources.FileResource
import org.apache.tools.ant.types.resources.Union
import org.gradle.api.tasks.OutputFile
import org.opensearch.gradle.AntTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.IgnoreEmptyDirectories
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SkipWhenEmpty

/**
 * Checks files for license headers.
 * <p>
 */
class LicenseHeadersTask extends AntTask {

    @OutputFile
    File reportFile = new File(project.buildDir, 'reports/licenseHeaders/rat1.log')

    /** Allowed license families for this project. */
    @Input
    List<String> approvedLicenses = ['Apache', 'Generated', 'SPDX', 'Vendored']

    /**
     * Files that should be excluded from the license header check. Use with extreme care, only in situations where the license on the
     * source file is compatible with the codebase but we do not want to add the license to the list of approved headers (to avoid the
     * possibility of inadvertently using the license on our own source files).
     */
    @Input
    List<String> excludes = []

    /**
     * Additional license families that may be found. The key is the license category name (5 characters),
     * followed by the family name and the value list of patterns to search for.
     */
    protected Map<String, String> additionalLicenses = new HashMap<>()

    LicenseHeadersTask() {
        description = "Checks sources for missing, incorrect, or unacceptable license headers"
    }

    /**
     * The list of java files to check. protected so the afterEvaluate closure in the
     * constructor can write to it.
     */
    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    List<FileCollection> getJavaFiles() {
        return project.sourceSets.collect({it.allJava})
    }

    /**
     * Create license matcher from allowed/disallowed license list.
     *
     * @param licenseSettingsMap A map of license identifier and its associated data (family name, category and pattern)
     */
    private static ILicense generateRatLicense(String licenseCategory, String licenseFamilyName, String pattern) {
        SortedSet<ILicenseFamily> licenseCtx = new TreeSet<ILicenseFamily>()
        var licenseFamilyBuilder = new ILicenseFamilyBuilder()
        var licenseFamily = licenseFamilyBuilder.setLicenseFamilyCategory(licenseCategory)
            .setLicenseFamilyName(licenseFamilyName)
            .build()
        licenseCtx.add(licenseFamily)

        var license = new License()
        license.setName(licenseFamily.getFamilyName())
        license.setFamily(licenseFamily.getFamilyCategory())
        license.add(new SimpleTextMatcher(pattern))

        var configuredLicense = license.build(licenseCtx)

        return configuredLicense
    }

    /**
     * Add a new license type.
     *
     * The license may be added to the {@link #approvedLicenses} using the {@code familyName}.
     *
     * @param categoryName A 5-character string identifier for the license
     * @param familyName An expanded string name for the license
     * @param pattern A pattern to search for, which if found, indicates a file contains the license
     */
    void additionalLicense(String categoryName, String familyName, String pattern) {
        if (categoryName.length() != 5) {
            throw new IllegalArgumentException("License category name must be exactly 5 characters, got ${categoryName}");
        }
        additionalLicenses.put(categoryName + familyName, pattern);
    }

    @Override
    protected void runAnt(AntBuilder ant) {
        List<ILicense> approvedLicenses = List.of(
            // Apache 2
            generateRatLicense("AL2", "Apache License Version 2.0", "Licensed to the Apache Software Foundation (ASF)"),
            // Generated code from Protocol Buffer compiler
            generateRatLicense("GEN", "Generated", "Generated by the protocol buffer compiler.  DO NOT EDIT!"),
            // Apache (ES)
            generateRatLicense("AL", "Apache", "Licensed to Elasticsearch under one or more contributor"),
            // SPDX
            generateRatLicense("SPDX", "SPDX", "SPDX-License-Identifier: Apache-2.0"),
            // SPDX: Apache license (OpenSearch)
            generateRatLicense("SPDX-ES", "SPDX", "Copyright OpenSearch Contributors."),
            // Generated resources
            generateRatLicense("GEN", "Generated", "ANTLR GENERATED CODE"),
            // Vendored code
            generateRatLicense("VEN", "Vendored", "@noticed"),
        )

        // Currently Apache Rat doesn't display header for source code file with negative matching
        // like BSD4
        // Source: https://github.com/apache/creadur-rat/blob/apache-rat-project-0.16.1/apache-rat-core/src/main/resources/org/apache/rat/plain-rat.xsl#L85-L87)
        // Uncomment and integrate the negative matcher for BSD4 once Rat supports
        // List<ILicense> disapprovedLicenses = List.of(
        //    // BSD 4-clause stuff (is disallowed below)
        //    // we keep this here, in case someone adds BSD code for some reason, it should never be allowed.
        //    generateRatLicense("BSD4", "Original BSD License (with advertising clause)", "All advertising materials"),
        // )

        ReportConfiguration configuration = new ReportConfiguration(DefaultLog.INSTANCE);
        configuration.setOut(reportFile)
        configuration.setStyleSheet(Defaults.getPlainStyleSheet())
        configuration.addLicensesIfNotPresent(approvedLicenses)
        configuration.addApprovedLicenseCategories(approvedLicenses.stream().map(l -> l.getLicenseFamily().getFamilyCategory()).toList())

        // License types added by the project
        for (Map.Entry<String, String> additional : additionalLicenses.entrySet()) {
            String category = additional.getKey().substring(0, 5)
            String family = additional.getKey().substring(5)
            configuration.addLicense(generateRatLicense(
                category,
                family,
                additional.getValue(),
            ))
            configuration.addApprovedLicenseCategory(category)
        }

        Union union = new Union()
        for (FileCollection dirSet : javaFiles) {
            for (File file: dirSet) {
                union.add(new FileResource(file))
            }
        }
        configuration.setReportable(new ResourceCollectionContainer(union))
        Reporter.report(configuration)

        // check the license file for any errors, this should be fast.
        boolean zeroUnknownLicenses = false
        boolean foundProblemsWithFiles = false
        reportFile.eachLine('UTF-8') { line ->
            if (line.startsWith("0 Unknown Licenses")) {
                zeroUnknownLicenses = true
            }
            if (line.startsWith(" !")) {
                foundProblemsWithFiles = true
            }
        }

        if (zeroUnknownLicenses == false || foundProblemsWithFiles) {
            // print the unapproved license section, usually its all you need to fix problems.
            int sectionNumber = 0
            reportFile.eachLine('UTF-8') { line ->
                if (line.startsWith("*******************************")) {
                    sectionNumber++
                } else {
                    if (sectionNumber == 2) {
                        logger.error(line)
                    }
                }
            }
            throw new IllegalStateException("License header problems were found! Full details: " + reportFile.absolutePath)
        }
    }
}
