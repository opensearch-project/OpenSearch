/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle.precommit;

import org.apache.rat.Defaults;
import org.apache.rat.ReportConfiguration;
import org.apache.rat.Reporter;
import org.apache.rat.document.DocumentName;
import org.apache.rat.document.DocumentNameMatcher;
import org.apache.rat.document.FileDocument;
import org.apache.rat.report.claim.ClaimStatistic;
import org.apache.rat.utils.DefaultLog;
import org.apache.rat.utils.Log;
import org.apache.rat.walker.DirectoryWalker;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.SourceDirectorySet;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;

import javax.inject.Inject;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Checks files for license headers.
 * <p>
 * The license definitions live in {@code rat-config.xml} rather than in this task, so that the
 * same definitions can be used by the Rat command line or any other Rat UI. This task reads that
 * file, walks the Java source directories and fails the build if any file has an unknown or
 * unapproved license header.
 */
public class LicenseHeadersTask extends DefaultTask {

    /** The name of the Rat configuration resource that holds our license definitions. */
    private static final String CONFIG_RESOURCE = "/rat-config.xml";

    /** The number of characters a Rat license family category must have. */
    private static final int CATEGORY_LENGTH = 5;

    /**
     * The java source directories to scan, captured at configuration time so that the task action
     * does not need to reach back into the project.
     */
    private final List<FileCollection> javaFiles;

    /**
     * Additional license families that may be found. The key is the license category name
     * (5 characters), followed by the family name, and the value is the pattern to search for.
     */
    protected Map<String, String> additionalLicenses = new LinkedHashMap<>();

    /**
     * Files that should be excluded from the license header check. Use with extreme care, only in
     * situations where the license on the source file is compatible with the codebase but we do not
     * want to add the license to the list of approved headers (to avoid the possibility of
     * inadvertently using the license on our own source files).
     */
    private List<String> excludes = new ArrayList<>();

    private final File reportDir;

    private File reportFile;

    @Inject
    public LicenseHeadersTask(Project project) {
        setDescription("Checks sources for missing, incorrect, or unacceptable license headers");
        this.reportDir = project.getLayout().getBuildDirectory().dir("reports/licenseHeaders").get().getAsFile();
        this.reportFile = new File(reportDir, "rat.log");
        this.javaFiles = project.getExtensions()
            .getByType(SourceSetContainer.class)
            .stream()
            .map(sourceSet -> (FileCollection) sourceSet.getAllJava())
            .collect(Collectors.toList());
    }

    @OutputFile
    public File getReportFile() {
        return reportFile;
    }

    public void setReportFile(File reportFile) {
        this.reportFile = reportFile;
    }

    @Input
    public List<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(List<String> excludes) {
        this.excludes = excludes;
    }

    /**
     * The list of java files to check.
     */
    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    public List<FileCollection> getJavaFiles() {
        return javaFiles;
    }

    /**
     * Additional license families that may be found, keyed by category name plus family name.
     */
    @Input
    public Map<String, String> getAdditionalLicenses() {
        return additionalLicenses;
    }

    /**
     * Add a new license type.
     * <p>
     * The license may be added to the approved licenses using the {@code familyName}.
     *
     * @param categoryName A 5-character string identifier for the license
     * @param familyName An expanded string name for the license
     * @param pattern A pattern to search for, which if found, indicates a file contains the license
     */
    public void additionalLicense(String categoryName, String familyName, String pattern) {
        if (categoryName.length() != CATEGORY_LENGTH) {
            throw new IllegalArgumentException("License category name must be exactly 5 characters, got " + categoryName);
        }
        additionalLicenses.put(categoryName + familyName, pattern);
    }

    @TaskAction
    public void checkLicenseHeaders() throws IOException {
        // Route Rat's logging through Gradle's logger rather than stdout.
        Log previousLog = DefaultLog.setInstance(new GradleLog(getLogger()));
        try {
            Files.deleteIfExists(reportFile.toPath());

            ReportConfiguration configuration = new ReportConfiguration();
            // Use only our own definitions, not the ones Rat ships with, so that a license we have
            // not explicitly approved is never silently accepted.
            configuration.setFrom(Defaults.builder().noDefault().add(ratConfig()).build());
            configuration.setOut(reportFile);

            if (excludes.isEmpty() == false) {
                configuration.addExcludedPatterns(excludes);
            }

            for (File dir : sourceDirectories()) {
                // Exclude patterns are relative to the source directory being scanned, matching the
                // fileset semantics this task had when it was driven through Ant.
                DocumentName dirName = DocumentName.builder(dir).build();
                DocumentNameMatcher excluder = configuration.getDocumentExcluder(dirName);
                configuration.addSource(new DirectoryWalker(new FileDocument(dirName, dir, excluder)));
            }

            if (configuration.hasSource() == false) {
                // Nothing to check. Every source directory was missing, e.g. a project with no java sources.
                return;
            }

            Reporter reporter = new Reporter(configuration);
            ClaimStatistic statistics;
            try {
                statistics = reporter.output();
            } catch (Exception e) {
                throw new GradleException("Could not execute Rat report", e);
            }

            int unapproved = statistics.getCounter(ClaimStatistic.Counter.UNAPPROVED);
            int unknown = statistics.getCounter(ClaimStatistic.Counter.UNKNOWN);
            if (unapproved > 0 || unknown > 0) {
                for (String offender : unapprovedFiles(reporter)) {
                    getLogger().error(offender);
                }
                throw new GradleException(
                    String.format(
                        "License header problems were found! %d unapproved, %d unknown. Full details: %s",
                        unapproved,
                        unknown,
                        reportFile.getAbsolutePath()
                    )
                );
            }
        } finally {
            DefaultLog.setInstance(previousLog);
        }
    }

    /**
     * Materializes the Rat configuration into the build directory, applying any licenses registered
     * through {@link #additionalLicense}. The definitions ship as a classpath resource inside the
     * build-tools jar, and Rat needs a {@code File}, so we always write a copy out.
     */
    private File ratConfig() {
        URL resource = LicenseHeadersTask.class.getResource(CONFIG_RESOURCE);
        if (resource == null) {
            throw new GradleException("Could not find " + CONFIG_RESOURCE + " on the classpath");
        }

        Document document;
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            document = factory.newDocumentBuilder().parse(resource.toExternalForm());
        } catch (Exception e) {
            throw new GradleException("Could not parse " + CONFIG_RESOURCE, e);
        }

        for (Map.Entry<String, String> additional : additionalLicenses.entrySet()) {
            String category = additional.getKey().substring(0, CATEGORY_LENGTH);
            String family = additional.getKey().substring(CATEGORY_LENGTH);
            addLicense(document, category.trim(), family, additional.getValue());
        }

        File generated = new File(reportDir, "rat-config.xml");
        try {
            Files.createDirectories(generated.getParentFile().toPath());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            transformerFactory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            transformerFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.transform(new DOMSource(document), new StreamResult(generated));
        } catch (Exception e) {
            throw new GradleException("Could not write " + generated, e);
        }
        return generated;
    }

    /**
     * Adds a family, a license with a single text matcher, and an approval entry to the config document.
     */
    private void addLicense(Document document, String category, String familyName, String pattern) {
        Element family = document.createElement("family");
        family.setAttribute("id", category);
        family.setAttribute("name", familyName);
        firstChild(document, "families").appendChild(family);

        Element text = document.createElement("text");
        text.setTextContent(pattern);

        Element license = document.createElement("license");
        license.setAttribute("family", category);
        license.setAttribute("name", familyName);
        license.appendChild(text);
        firstChild(document, "licenses").appendChild(license);

        Element approved = document.createElement("family");
        approved.setAttribute("license_ref", category);
        firstChild(document, "approved").appendChild(approved);
    }

    private Element firstChild(Document document, String tagName) {
        NodeList elements = document.getElementsByTagName(tagName);
        if (elements.getLength() == 0) {
            throw new GradleException("Could not find <" + tagName + "> in " + CONFIG_RESOURCE);
        }
        return (Element) elements.item(0);
    }

    /**
     * Extracts the names of the files with unapproved licenses from the report document, so that the
     * failure names the offending files rather than only pointing at the report.
     */
    private List<String> unapprovedFiles(Reporter reporter) {
        List<String> result = new ArrayList<>();
        Document document = reporter.getDocument();
        if (document == null) {
            return result;
        }
        NodeList resources = document.getElementsByTagName("resource");
        for (int i = 0; i < resources.getLength(); i++) {
            Element resource = (Element) resources.item(i);
            NodeList licenses = resource.getElementsByTagName("license");
            for (int j = 0; j < licenses.getLength(); j++) {
                Element license = (Element) licenses.item(j);
                if ("false".equals(license.getAttribute("approval"))) {
                    result.add(String.format("  %s (%s)", resource.getAttribute("name"), license.getAttribute("name")));
                    break;
                }
            }
        }
        return result;
    }

    /**
     * The source directories to scan, skipping any that do not exist. Sometimes these dirs don't
     * exist, e.g. site-plugin has no actual java src/main.
     */
    private List<File> sourceDirectories() {
        List<File> result = new ArrayList<>();
        for (FileCollection files : javaFiles) {
            for (File dir : ((SourceDirectorySet) files).getSrcDirs()) {
                if (dir.exists()) {
                    result.add(dir);
                }
            }
        }
        return result;
    }

    /**
     * Adapts Rat's logging to the Gradle logger.
     */
    private static class GradleLog implements Log {
        private final org.gradle.api.logging.Logger gradleLogger;

        GradleLog(org.gradle.api.logging.Logger gradleLogger) {
            this.gradleLogger = gradleLogger;
        }

        @Override
        public Level getLevel() {
            if (gradleLogger.isDebugEnabled()) {
                return Level.DEBUG;
            } else if (gradleLogger.isInfoEnabled()) {
                return Level.INFO;
            } else if (gradleLogger.isWarnEnabled()) {
                return Level.WARN;
            } else if (gradleLogger.isErrorEnabled()) {
                return Level.ERROR;
            }
            return Level.OFF;
        }

        @Override
        public void log(Level level, String message) {
            switch (level) {
                case DEBUG:
                    gradleLogger.debug(message);
                    break;
                case INFO:
                    gradleLogger.info(message);
                    break;
                case WARN:
                    gradleLogger.warn(message);
                    break;
                case ERROR:
                    gradleLogger.error(message);
                    break;
                case OFF:
                default:
                    break;
            }
        }
    }
}
