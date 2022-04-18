package org.opensearch.gradle.zipplugin;

import java.util.*;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.provider.Property;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.provider.Provider;
import java.nio.file.Path;
import org.opensearch.gradle.zipplugin.ZipPublishExtension;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.Task;
import org.opensearch.gradle.zipplugin.ZipPublishUtil;


public class ZipPublish implements Plugin<Project> {
    private Project project;

    static final String EXTENSION_NAME = "zipmavensettings";
    public final static String PUBLICATION_NAME = "mavenzip";
    public final static String STAGING_REPO = "zipstaging";
    public final static String MAVEN_ZIP_PUBLISH_TASK = "publish" + ZipPublishUtil.capitalize(PUBLICATION_NAME) + "PublicationTo" + ZipPublishUtil.capitalize(STAGING_REPO) + "Repository";
    public final static String MAVEN_ZIP_PUBLISH_POM_TASK  = "generatePomFileFor" + ZipPublishUtil.capitalize(PUBLICATION_NAME) + "Publication";
    public final static String LOCALMAVEN = "publishToMavenLocal";

    private void configMaven() {
        final Path buildDirectory = this.project.getRootDir().toPath();
        this.project.getPluginManager().apply(MavenPublishPlugin.class);
        this.project.getExtensions().configure(PublishingExtension.class, publishing -> {
            publishing.repositories(repositories -> {
                repositories.maven(maven -> {
                    maven.setName(STAGING_REPO);
                    maven.setUrl(buildDirectory.toString() + "/build/local-staging-repo");
                });
            });
            System.out.println("Starting " + MAVEN_ZIP_PUBLISH_TASK + " task");
            publishing.publications(publications -> {
                publications.create(PUBLICATION_NAME, MavenPublication.class, mavenZip -> {
                    ZipPublishExtension extset = this.project.getExtensions().findByType(ZipPublishExtension.class);
                    //Getting the Zip group from created extension
                    String zipGroup = extset.getZipgroup();
                    String zipArtifact = getProperty("zipArtifact");
                    //Getting the Zip version from gradle property with/without added snapshot and qualifier
                    String zipVersion = getProperty("zipVersion");
                    String version = "";
                    String extra = "";
                    if (zipVersion != null){
                        StringTokenizer st = new StringTokenizer(zipVersion);  
                        version = st.nextToken("-")  + ".0";
                        try {
                            extra = zipVersion.substring(zipVersion.indexOf("-"));
                        } catch (Exception e) {
                            System.out.println("");
                        }
                    };
                    String finalZipVersion = version + extra;
                    String zipFilePath = "/build/distributions/" + zipArtifact + "-" + finalZipVersion + ".zip";
                    //-PzipFilePath=/build/distributions/opensearch-job-scheduler-2.0.0.0-alpha1-SNAPSHOT.zip
                    mavenZip.artifact(buildDirectory.toString() + zipFilePath);
                    mavenZip.setGroupId(zipGroup);
                    mavenZip.setArtifactId(zipArtifact);
                    mavenZip.setVersion(finalZipVersion);
                });
            });
        });
    }
    private String getProperty(String name) {
        if (this.project.hasProperty(name)) {
            Object property = this.project.property(name);
            if (property != null) {
                return property.toString();
            }
        }
        return null;
    }

    @Override
    public void apply(Project project) {
        final Path buildDirectory = project.getRootDir().toPath();
        this.project = project;
        project.getExtensions().create(EXTENSION_NAME, ZipPublishExtension.class);
        configMaven();
        Task compileJava = project.getTasks().findByName("compileJava");
        if(compileJava != null) {
            compileJava.setEnabled(false);
        }
        Task sourceJarTask = project.getTasks().findByName("sourcesJar");
        if(sourceJarTask != null) {
            sourceJarTask.setEnabled(false);
        }
        Task javaDocJarTask = project.getTasks().findByName("javadocJar");
        if(javaDocJarTask != null) {
            javaDocJarTask.setEnabled(false);
        }
        project.getGradle().getTaskGraph().whenReady(graph -> {
            if (graph.hasTask(LOCALMAVEN)){
                project.getTasks().getByName(MAVEN_ZIP_PUBLISH_POM_TASK).setEnabled(false);
            }

        });
    }
}
