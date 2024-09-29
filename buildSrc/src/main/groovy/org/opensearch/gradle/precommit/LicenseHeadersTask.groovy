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

import org.apache.rat.analysis.HeaderCheckWorker
import org.apache.rat.analysis.matchers.SimpleTextMatcher
import org.apache.rat.anttasks.License
import org.apache.rat.api.MetaData
import org.apache.rat.document.impl.FileDocument
import org.apache.rat.license.ILicense
import org.apache.rat.license.ILicenseFamily
import org.apache.rat.license.ILicenseFamilyBuilder
import org.opensearch.gradle.AntTask
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SkipWhenEmpty

/**
 * Checks files for license headers.
 * <p>
 */
class LicenseHeadersTask extends AntTask {

    /** Allowed license families for this project. */
    @Input
    List<String> approvedLicenses = ['Apache', 'Generated', 'SPDX', 'Vendored']

    /** Disallowed license families for this project. BSD4 at the moment */
    @Internal
    List<String> disapprovedLicenses = ['Original BSD License (with advertising clause)']

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
     *  Internal class to hold license metadata
     */
    private class LicenseSettings {
        private String licenseCategory
        private String licenseFamilyName
        private String pattern
        private LicenseSettings(String licenseCategory, String licenseFamilyName, String pattern) {
            this.licenseCategory = licenseCategory
            this.licenseFamilyName = licenseFamilyName
            this.pattern = pattern
        }
    }

    /**
     * Create license matcher from allowed/disallowed license list.
     *
     * @param licenseSettingsMap A map of license identifier and its associated data (family name, category and pattern)
     */
    private Map<String, ILicense> getLicenseMatchers(Map<String, LicenseSettings> licenseSettingsMap) {
        Map<String, ILicense> licenseHashMap = new HashMap<>()

        for (Map.Entry<String, LicenseSettings> entry: licenseSettingsMap.entrySet()) {
            SortedSet<ILicenseFamily> licenseCtx = new TreeSet<ILicenseFamily>()
            var licenseCodeName = entry.getKey()
            var licenseSetting = entry.getValue()
            var licenseFamilyBuilder = new ILicenseFamilyBuilder()
            var licenseFamily = licenseFamilyBuilder.setLicenseFamilyCategory(licenseSetting.licenseCategory)
                .setLicenseFamilyName(licenseSetting.licenseFamilyName)
                .build()
            licenseCtx.add(licenseFamily)

            var license = new License()
            license.setName(licenseFamily.getFamilyName())
            license.setFamily(licenseFamily.getFamilyCategory())
            license.add(new SimpleTextMatcher(licenseSetting.pattern))

            var configuredLicense = license.build(licenseCtx)
            licenseHashMap.put(licenseCodeName, configuredLicense)
        }

        return licenseHashMap
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
        Map<String, LicenseSettings> licenseSettingsHashMap = new HashMap<>()
        Map<String, List<String>> licenseStats = new HashMap<>()

        // BSD 4-clause stuff (is disallowed below)
        // we keep this here, in case someone adds BSD code for some reason, it should never be allowed.
        licenseSettingsHashMap.put("BSD4", new LicenseSettings("BSD4", "Original BSD License (with advertising clause)", "All advertising materials"))
        // Apache
        licenseSettingsHashMap.put("AL", new LicenseSettings("AL", "Apache", "Licensed to Elasticsearch under one or more contributor"))
        // SPDX
        licenseSettingsHashMap.put("SPDX1", new LicenseSettings("SPDX", "SPDX", "SPDX-License-Identifier: Apache-2.0"))
        // SPDX: Apache license (OpenSearch)
        licenseSettingsHashMap.put("SPDX2", new LicenseSettings("SPDX", "SPDX", "Copyright OpenSearch Contributors."))
        // Generated resources
        licenseSettingsHashMap.put("GEN", new LicenseSettings("GEN", "Generated", "ANTLR GENERATED CODE"))
        // Vendored code
        licenseSettingsHashMap.put("VEN", new LicenseSettings("VEN", "Vendored", "@noticed"))

        // License types added by the project
        for (Map.Entry<String, String> additional : additionalLicenses.entrySet()) {
            String category = additional.getKey().substring(0, 5)
            String family = additional.getKey().substring(5)
            licenseSettingsHashMap.put(category, new LicenseSettings(
                category,
                family,
                additional.getValue(),
            ))
        }

        Map<String, ILicense> licenseHashMap = this.getLicenseMatchers(licenseSettingsHashMap);

        for (FileCollection dirSet : javaFiles) {
            for (File file: dirSet) {
                var ratDoc = new FileDocument(file)
                var detectedLicenseFamilyName = null
                var detectedLicenseCodename = null
                for (Map.Entry<String, ILicense> entry: licenseHashMap.entrySet()) {
                    try (Reader reader = ratDoc.reader()) {
                        var worker = new HeaderCheckWorker(reader, HeaderCheckWorker.DEFAULT_NUMBER_OF_RETAINED_HEADER_LINES, entry.getValue(), ratDoc)
                        worker.read()
                        detectedLicenseFamilyName = ratDoc.getMetaData().get(MetaData.RAT_URL_LICENSE_FAMILY_NAME).value
                        if (!detectedLicenseFamilyName.equals("Unknown license")) {
                            detectedLicenseCodename = entry.getKey()
                            break;
                        }
                    }
                }
                var sourceFilePath = file.getCanonicalPath()
                if (disapprovedLicenses.contains(detectedLicenseFamilyName)) {
                    licenseStats.computeIfAbsent('DISAPPROVED', k -> new ArrayList<>()).add("(" + detectedLicenseCodename + ") " + sourceFilePath)
                } else if (detectedLicenseFamilyName.equals("Unknown license")) {
                    licenseStats.computeIfAbsent('MISSING/UNKNOWN', k -> new ArrayList<>()).add(sourceFilePath)
                }
            }
        }

        // check the license file for any errors, this should be fast.
        boolean hasDisapprovedLicenses = licenseStats.containsKey('DISAPPROVED') && licenseStats.get('DISAPPROVED').size() > 0
        boolean hasUnknownLicenses = licenseStats.containsKey('MISSING/UNKNOWN') && licenseStats.get('MISSING/UNKNOWN').size() > 0

        if (hasDisapprovedLicenses || hasUnknownLicenses)  {
            for (Map.Entry<String, List<String>> entry: licenseStats.entrySet()) {
                for (String line: entry.getValue()) {
                    logger.error(entry.getKey() + " " + line)
                }
            }
            throw new IllegalStateException("License header problems were found!")
        }
    }
}
