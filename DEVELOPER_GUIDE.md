# Developer Guide

So you want to contribute code to OpenSearch? Excellent! We're glad you're here. Here's what you need to do.

- [Getting Started](#getting-started)
    - [Git Clone OpenSearch Repo](#git-clone-opensearch-repo)
    - [Prerequisites](#prerequisites)
    - [Run Tests](#run-tests)
    - [Run OpenSearch](#run-opensearch)
- [Use an Editor](#use-an-editor)
    - [IntelliJ IDEA](#intellij-idea)
    - [Visual Studio Code](#visual-studio-code)
    - [Eclipse](#eclipse)
- [Project Layout](#project-layout)
    - [docs](#docs)
    - [distribution](#distribution)
    - [libs](#libs)
    - [modules](#modules)
    - [plugins](#plugins)
    - [qa](#qa)
    - [server](#server)
    - [test](#test)
- [Java Language Formatting Guidelines](#java-language-formatting-guidelines)
    - [Editor / IDE Support](#editor--ide-support)
    - [Formatting Failures](#formatting-failures)
- [Gradle Build](#gradle-build)
    - [Configurations](#configurations)
- [Misc](#misc)
    - [git-secrets](#git-secrets)
- [Submitting Changes](#submitting-changes)

## Getting Started

### Git Clone OpenSearch Repo

Fork [opensearch-project/OpenSearch](https://github.com/opensearch-project/OpenSearch) and clone locally, e.g. `git clone https://github.com/[username]/OpenSearch.git`.

### Prerequisites

#### JDK 14

OpenSearch builds using Java 14 at a minimum. This means you must have a JDK 14 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 14 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-14`.

By default, tests use the same runtime as `JAVA_HOME`. However, since OpenSearch supports JDK 8, the build supports compiling with JDK 14 and testing on a different version of JDK runtime. To do this, set `RUNTIME_JAVA_HOME` pointing to the Java home of another JDK installation, e.g. `RUNTIME_JAVA_HOME=/usr/lib/jvm/jdk-8`.

To run the full suite of tests you will also need `JAVA8_HOME`, `JAVA9_HOME`, `JAVA10_HOME`, `JAVA11_HOME`, and `JAVA12_HOME`.

#### Docker

[Docker](https://docs.docker.com/install/) is required for building some OpenSearch artifacts and executing certain test suites.

### Run Tests

OpenSearch uses a Gradle wrapper for its build. Run `gradlew` on Unix systems, or `gradlew.bat` on Windows in the root of the repository.

Start by running the test suite with `gradlew check`. This should complete without errors and takes ~15 minutes on a c5.18xlarge.

```
./gradlew check

=======================================
OpenSearch Build Hamster says Hello!
  Gradle Version        : 6.6.1
  OS Info               : Linux 5.4.0-1037-aws (amd64)
  JDK Version           : 14 (JDK)
  JAVA_HOME             : /usr/lib/jvm/java-14-openjdk-amd64
=======================================

...

BUILD SUCCESSFUL in 14m 50s
2587 actionable tasks: 2450 executed, 137 up-to-date
```

**Note: OpenSearch hasn't made any changes to the test suite yet, beyond fixing tests that broke after removing non-Apache licensed code and non-Apache licensed code checks. Also, while we're in pre-alpha, some tests may be failing until we finish the forking process. We should have an issue for all failing tests, but if you find one first, feel free to open one (and fix it :) ).**

If the full test suite fails you may want to start with a smaller set.

```
./gradlew precommit
```

### Run OpenSearch

Run OpenSearch using `gradlew run`.

```
./gradlew run
```

That will build OpenSearch and start it, writing its log above Gradle's status message. We log a lot of stuff on startup, specifically these lines tell you that OpenSearch is ready.

```
[2020-05-29T14:50:35,167][INFO ][o.e.h.AbstractHttpServerTransport] [runTask-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
[2020-05-29T14:50:35,169][INFO ][o.e.n.Node               ] [runTask-0] started
```

It's typically easier to wait until the console stops scrolling, and then run `curl` in another window to check if OpenSearch instance is running.

```
curl -u opensearch:password localhost:9200

{
  "name" : "runTask-0",
  "cluster_name" : "runTask",
  "cluster_uuid" : "oX_S6cxGSgOr_mNnUxO6yQ",
  "version" : {
    "number" : "1.0.0-SNAPSHOT",
    "build_type" : "tar",
    "build_hash" : "0ba0e7cc26060f964fcbf6ee45bae53b3a9941d0",
    "build_date" : "2021-04-16T19:45:44.248303Z",
    "build_snapshot" : true,
    "lucene_version" : "8.7.0",
    "minimum_wire_compatibility_version" : "6.8.0",
    "minimum_index_compatibility_version" : "6.0.0-beta1"
  }
}
```

## Use an Editor

### IntelliJ IDEA

When importing into IntelliJ you will need to define an appropriate JDK. The convention is that **this SDK should be named "14"**, and the project import will detect it automatically. For more details on defining an SDK in IntelliJ please refer to [this documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk). Note that SDK definitions are global, so you can add the JDK from any project, or after project import. Importing with a missing JDK will still work, IntelliJ will report a problem and will refuse to build until resolved.

You can import the OpenSearch project into IntelliJ IDEA as follows.

1. Select **File > Open**
2. In the subsequent dialog navigate to the root `build.gradle` file
3. In the subsequent dialog select **Open as Project**

### Visual Studio Code

Follow links in the [Java Tutorial](https://code.visualstudio.com/docs/java/java-tutorial) to install the coding pack and extensions for Java, Gradle tasks, etc. Open the source code directory.

### Eclipse

We would like to support Eclipse, but few of us use it and has fallen into disrepair. Please [contribute](CONTRIBUTING.md).

## Project Layout

This repository is split into many top level directories. The most important ones are:

### `docs`

Documentation for the project.

### `distribution`

Builds our tar and zip archives and our rpm and deb packages.

### `libs`

Libraries used to build other parts of the project. These are meant to be internal rather than general purpose. We have no plans to
[semver](https://semver.org/) their APIs or accept feature requests for them. We publish them to maven central because they are dependencies of our plugin test framework, high level rest client, and jdbc driver but they really aren't general purpose enough to *belong* in maven central. We're still working out what to do here.

### `modules`

Features that are shipped with OpenSearch by default but are not built in to the server. We typically separate features from the server because they require permissions that we don't believe *all* of OpenSearch should have or because they depend on libraries that we don't believe *all* of OpenSearch should depend on.

For example, reindex requires the `connect` permission so it can perform reindex-from-remote but we don't believe that the *all* of OpenSearch should have the "connect". For another example, Painless is implemented using antlr4 and asm and we don't believe that *all* of OpenSearch should have access to them.

### `plugins`

Officially supported plugins to OpenSearch. We decide that a feature should be a plugin rather than shipped as a module because we feel that it is only important to a subset of users, especially if it requires extra dependencies.

The canonical example of this is the ICU analysis plugin. It is important for folks who want the fairly language neutral ICU analyzer but the library to implement the analyzer is 11MB so we don't ship it with OpenSearch by default.

Another example is the `discovery-gce` plugin. It is *vital* to folks running in [GCP](https://cloud.google.com/) but useless otherwise and it depends on a dozen extra jars.

### `qa`

Honestly this is kind of in flux and we're not 100% sure where we'll end up. We welcome your thoughts and help.

Right now the directory contains the following.

* Tests that require multiple modules or plugins to work.
* Tests that form a cluster made up of multiple versions of OpenSearch like full cluster restart, rolling restarts, and mixed version tests.
* Tests that test the OpenSearch clients in "interesting" places like the `wildfly` project.
* Tests that test OpenSearch in funny configurations like with ingest disabled.
* Tests that need to do strange things like install plugins that thrown uncaught `Throwable`s or add a shutdown hook.

But we're not convinced that all of these things *belong* in the qa directory. We're fairly sure that tests that require multiple modules or plugins to work should just pick a "home" plugin. We're fairly sure that the multi-version tests *do* belong in qa. Beyond that, we're not sure. If you want to add a new qa project, open a PR and be ready to discuss options.

### `server`

The server component of OpenSearch that contains all of the modules and plugins. Right now things like the high level rest client depend on the server but we'd like to fix that in the future.

### `test`

Our test framework and test fixtures. We use the test framework for testing the server, the plugins, and modules, and pretty much everything else. We publish the test framework so folks who develop OpenSearch plugins can use it to test the plugins. The test fixtures are external processes that we start before running specific tests that rely on them.

For example, we have an hdfs test that uses mini-hdfs to test our repository-hdfs plugin.

## Java Language Formatting Guidelines

Java files in the OpenSearch codebase are formatted with the Eclipse JDT formatter, using the [Spotless Gradle](https://github.com/diffplug/spotless/tree/master/plugin-gradle) plugin. This plugin is configured on a project-by-project basis, via `build.gradle` in the root of the repository. So long as at least one project is configured, the formatting check can be run explicitly with:

    ./gradlew spotlessJavaCheck

The code can be formatted with:

    ./gradlew spotlessApply

These tasks can also be run for specific subprojects, e.g.

    ./gradlew server:spotlessJavaCheck

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* Lines of code surrounded by `// tag::NAME` and `// end::NAME` comments are included in the documentation and should only be 76 characters wide not counting leading indentation. Such regions of code are not formatted automatically as it is not possible to change the line length rule of the formatter for part of a file. Please format such sections sympathetically with the rest of the code, while keeping lines to maximum length of 76 characters.
* Wildcard imports (`import foo.bar.baz.*`) are forbidden and will cause the build to fail.
* If *absolutely* necessary, you can disable formatting for regions of code with the `// tag::NAME` and `// end::NAME` directives, but note that these are intended for use in documentation, so please make it clear what you have done, and only do this where the benefit clearly outweighs the decrease in consistency.
* Note that JavaDoc and block comments i.e. `/* ... */` are not formatted, but line comments i.e `// ...` are.
* There is an implicit rule that negative boolean expressions should use the form `foo == false` instead of `!foo` for better readability of the code. While this isn't strictly enforced, if might get called out in PR reviews as something to change.

### Editor / IDE Support

IntelliJ IDEs can [import](https://blog.jetbrains.com/idea/2014/01/intellij-idea-13-importing-code-formatter-settings-from-eclipse/) the same settings file, and / or use the [Eclipse Code Formatter](https://plugins.jetbrains.com/plugin/6546-eclipse-code-formatter)
plugin.

You can also tell Spotless to [format a specific file](https://github.com/diffplug/spotless/tree/master/plugin-gradle#can-i-apply-spotless-to-specific-files) from the command line.

### Formatting Failures

Sometimes Spotless will report a "misbehaving rule which can't make up its mind" and will recommend enabling the `paddedCell()` setting. If you enabled this settings and run the format check again, Spotless will write files to `$PROJECT/build/spotless-diagnose-java/` to aid diagnosis. It writes different copies of the formatted files, so that you can see how they
differ and infer what is the problem.

The `paddedCell()` option is disabled for normal operation in order to detect any misbehaviour. You can enable the option from the command line by running Gradle with `-Dspotless.paddedcell`.

> **NOTE:** If you have imported the project into IntelliJ IDEA the project will be automatically configured to add the correct license header to new source files based on the source location.

## Gradle Build

We use Gradle to build OpenSearch because it is flexible enough to not only build and package OpenSearch, but also orchestrate all of the ways that we have to test OpenSearch.

### Configurations

Gradle organizes dependencies and build artifacts into "configurations" and allows you to use these configurations arbitrarily. Here are some of the most common configurations in our build and how we use them:

#### implementation

Dependencies that are used by the project at compile and runtime but are not exposed as a compile dependency to other dependent projects. Dependencies added to the `implementation` configuration are considered an implementation detail that can be changed at a later date without affecting any dependent projects.

#### api

Dependencies that are used as compile and runtime dependencies of a project and are considered part of the external api of the project.

#### runtimeOnly

Dependencies that not on the classpath at compile time but are on the classpath at runtime. We mostly use this configuration to make sure that we do not accidentally compile against dependencies of our dependencies also known as "transitive" dependencies".

#### compileOnly

Code that is on the classpath at compile time but that should not be shipped with the project because it is "provided" by the runtime
somehow. OpenSearch plugins use this configuration to include dependencies that are bundled with OpenSearch's server.

#### testImplementation

Code that is on the classpath for compiling tests that are part of this project but not production code. The canonical example
of this is `junit`.

## Misc

### git-secrets

Security is our top priority. Avoid checking in credentials, install [awslabs/git-secrets](https://github.com/awslabs/git-secrets).

```
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install
```

## Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).
