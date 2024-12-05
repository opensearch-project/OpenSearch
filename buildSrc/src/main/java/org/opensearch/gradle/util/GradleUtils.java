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

package org.opensearch.gradle.util;

import org.opensearch.gradle.OpenSearchJavaPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.UnknownTaskException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.LenientConfiguration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.internal.artifacts.ivyservice.ResolvedFilesCollectingVisitor;
import org.gradle.api.internal.artifacts.ivyservice.resolveengine.artifact.SelectedArtifactSet;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceRegistration;
import org.gradle.api.services.BuildServiceRegistry;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.idea.model.IdeaModel;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class GradleUtils {

    public static <T> Action<T> noop() {
        return t -> {};
    }

    public static SourceSetContainer getJavaSourceSets(Project project) {
        return project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
    }

    public static <T extends Task> TaskProvider<T> maybeRegister(TaskContainer tasks, String name, Class<T> clazz, Action<T> action) {
        try {
            return tasks.named(name, clazz);
        } catch (UnknownTaskException e) {
            return tasks.register(name, clazz, action);
        }
    }

    public static void maybeConfigure(TaskContainer tasks, String name, Action<? super Task> config) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return;
        }

        task.configure(config);
    }

    public static <T extends Task> void maybeConfigure(
        TaskContainer tasks,
        String name,
        Class<? extends T> type,
        Action<? super T> config
    ) {
        tasks.withType(type).configureEach(task -> {
            if (task.getName().equals(name)) {
                config.execute(task);
            }
        });
    }

    public static TaskProvider<?> findByName(TaskContainer tasks, String name) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return null;
        }

        return task;
    }

    @SuppressWarnings("unchecked")
    public static <T extends BuildService<?>> Provider<T> getBuildService(BuildServiceRegistry registry, String name) {
        BuildServiceRegistration<?, ?> registration = registry.getRegistrations().findByName(name);
        if (registration == null) {
            throw new GradleException("Unable to find build service with name '" + name + "'.");
        }

        return (Provider<T>) registration.getService();
    }

    /**
     * Add a source set and task of the same name that runs tests.
     * <p>
     * IDEs are also configured if setup, and the test task is added to check. The new test source
     * set extends from the normal test source set to allow sharing of utilities.
     *
     * @return A task provider for the newly created test task
     */
    public static TaskProvider<?> addTestSourceSet(Project project, String sourceSetName) {
        project.getPluginManager().apply(OpenSearchJavaPlugin.class);

        // create our test source set and task
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet testSourceSet = sourceSets.create(sourceSetName);
        TaskProvider<Test> testTask = project.getTasks().register(sourceSetName, Test.class);
        testTask.configure(task -> {
            task.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            task.setClasspath(testSourceSet.getRuntimeClasspath());
            // make the new test run after unit tests
            task.mustRunAfter(project.getTasks().named("test"));
        });

        Configuration testCompileConfig = project.getConfigurations().getByName(testSourceSet.getCompileClasspathConfigurationName());
        Configuration testRuntimeConfig = project.getConfigurations().getByName(testSourceSet.getRuntimeClasspathConfigurationName());
        testSourceSet.setCompileClasspath(testCompileConfig);
        testSourceSet.setRuntimeClasspath(project.getObjects().fileCollection().from(testSourceSet.getOutput(), testRuntimeConfig));

        extendSourceSet(project, SourceSet.MAIN_SOURCE_SET_NAME, sourceSetName);

        setupIdeForTestSourceSet(project, testSourceSet);

        // add to the check task
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        return testTask;
    }

    public static void setupIdeForTestSourceSet(Project project, SourceSet testSourceSet) {
        // setup IDEs
        String runtimeClasspathName = testSourceSet.getRuntimeClasspathConfigurationName();
        Configuration runtimeClasspathConfiguration = project.getConfigurations().getByName(runtimeClasspathName);
        project.getPluginManager().withPlugin("idea", p -> {
            IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
            idea.getModule().setTestSourceDirs(testSourceSet.getJava().getSrcDirs());
            idea.getModule().getScopes().put(testSourceSet.getName(), new HashMap<String, Collection<Configuration>>() {
                {
                    put("plus", Arrays.asList(runtimeClasspathConfiguration));
                }
            });
        });
        project.getPluginManager().withPlugin("eclipse", p -> {
            EclipseModel eclipse = project.getExtensions().getByType(EclipseModel.class);
            List<SourceSet> eclipseSourceSets = new ArrayList<>();
            for (SourceSet old : eclipse.getClasspath().getSourceSets()) {
                eclipseSourceSets.add(old);
            }
            eclipseSourceSets.add(testSourceSet);
            eclipse.getClasspath().setSourceSets(project.getExtensions().getByType(SourceSetContainer.class));
            eclipse.getClasspath().getPlusConfigurations().add(runtimeClasspathConfiguration);
        });
    }

    /**
     * Extend the configurations of one source set from another.
     */
    public static void extendSourceSet(Project project, String parentSourceSetName, String childSourceSetName) {
        final List<Function<SourceSet, String>> configNameFunctions = Arrays.asList(
            SourceSet::getCompileClasspathConfigurationName,
            SourceSet::getImplementationConfigurationName,
            SourceSet::getRuntimeClasspathConfigurationName,
            SourceSet::getRuntimeOnlyConfigurationName
        );
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet parent = sourceSets.getByName(parentSourceSetName);
        SourceSet child = sourceSets.getByName(childSourceSetName);

        for (Function<SourceSet, String> configNameFunction : configNameFunctions) {
            String parentConfigName = configNameFunction.apply(parent);
            String childConfigName = configNameFunction.apply(child);
            Configuration parentConfig = project.getConfigurations().getByName(parentConfigName);
            Configuration childConfig = project.getConfigurations().getByName(childConfigName);
            childConfig.extendsFrom(parentConfig);
        }

        // tie this new test source set to the main and test source sets
        child.setCompileClasspath(project.getObjects().fileCollection().from(child.getCompileClasspath(), parent.getOutput()));
        child.setRuntimeClasspath(project.getObjects().fileCollection().from(child.getRuntimeClasspath(), parent.getOutput()));
    }

    /**
     * Extends one configuration from another and refreshes the classpath of a provided Test.
     * The Test parameter is only needed for eagerly defined test tasks.
     */
    public static void extendSourceSet(Project project, String parentSourceSetName, String childSourceSetName, Test test) {
        extendSourceSet(project, parentSourceSetName, childSourceSetName);
        if (test != null) {
            SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
            SourceSet child = sourceSets.getByName(childSourceSetName);
            test.setClasspath(child.getRuntimeClasspath());
        }
    }

    public static Dependency projectDependency(Project project, String projectPath, String projectConfig) {
        if (project.findProject(projectPath) == null) {
            throw new GradleException("no project [" + projectPath + "], project names: " + project.getRootProject().getAllprojects());
        }
        Map<String, Object> depConfig = new HashMap<>();
        depConfig.put("path", projectPath);
        depConfig.put("configuration", projectConfig);
        return project.getDependencies().project(depConfig);
    }

    /**
     * To calculate the project path from a task path without relying on Task#getProject() which is discouraged during
     * task execution time.
     */
    public static String getProjectPathFromTask(String taskPath) {
        int lastDelimiterIndex = taskPath.lastIndexOf(":");
        return lastDelimiterIndex == 0 ? ":" : taskPath.substring(0, lastDelimiterIndex);
    }

    public static FileCollection getFiles(Project project, Configuration cfg, Spec<Dependency> spec) {
        final LenientConfiguration configuration = cfg.getResolvedConfiguration().getLenientConfiguration();
        try {
            // Using reflection here to cover the pre 8.7 releases (since those have no such APIs), the
            // ResolverResults.LegacyResolverResults.LegacyVisitedArtifactSet::select(...) is not available
            // on older versions.
            final MethodHandle mh = MethodHandles.lookup()
                .findVirtual(configuration.getClass(), "select", MethodType.methodType(SelectedArtifactSet.class, Spec.class))
                .bindTo(configuration);

            final ResolvedFilesCollectingVisitor visitor = new ResolvedFilesCollectingVisitor();
            ((SelectedArtifactSet) mh.invoke(spec)).visitArtifacts(visitor, false);
            return project.files(visitor.getFiles());
        } catch (Throwable ex) {
            return project.files(configuration.getFiles(spec));
        }
    }
}
