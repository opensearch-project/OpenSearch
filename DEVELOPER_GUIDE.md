- [Developer Guide](#developer-guide)
  - [Getting Started](#getting-started)
    - [Git Clone OpenSearch Repo](#git-clone-opensearch-repo)
    - [Install Prerequisites](#install-prerequisites)
      - [JDK 11](#jdk-11)
      - [JDK 14](#jdk-14)
      - [Runtime JDK](#runtime-jdk)
      - [Windows](#windows)
      - [Docker](#docker)
    - [Build](#build)
    - [Run Tests](#run-tests)
    - [Run OpenSearch](#run-opensearch)
  - [Use an Editor](#use-an-editor)
    - [IntelliJ IDEA](#intellij-idea)
    - [Visual Studio Code](#visual-studio-code)
    - [Eclipse](#eclipse)
  - [Project Layout](#project-layout)
    - [`distribution`](#distribution)
    - [`libs`](#libs)
    - [`modules`](#modules)
    - [`plugins`](#plugins)
    - [`sandbox`](#sandbox)
    - [`qa`](#qa)
    - [`server`](#server)
    - [`test`](#test)
  - [Java Language Formatting Guidelines](#java-language-formatting-guidelines)
    - [Editor / IDE Support](#editor--ide-support)
    - [Formatting Failures](#formatting-failures)
  - [Gradle Build](#gradle-build)
    - [Configurations](#configurations)
      - [implementation](#implementation)
      - [api](#api)
      - [runtimeOnly](#runtimeonly)
      - [compileOnly](#compileonly)
      - [testImplementation](#testimplementation)
  - [Misc](#misc)
    - [git-secrets](#git-secrets)
      - [Installation](#installation)
      - [Configuration](#configuration)
  - [Components](#components)
    - [Build libraries & interfaces](#build-libraries--interfaces)
    - [Clients & Libraries](#clients--libraries)
    - [Plugins](#plugins-1)
    - [Indexing & search](#indexing--search)
    - [Aggregations](#aggregations)
    - [Distributed Framework](#distributed-framework)
  - [Submitting Changes](#submitting-changes)
  - [Backports](#backports)

# Developer Guide

So you want to contribute code to OpenSearch? Excellent! We're glad you're here. Here's what you need to do.

## Getting Started

### Git Clone OpenSearch Repo

Fork [opensearch-project/OpenSearch](https://github.com/opensearch-project/OpenSearch) and clone locally, e.g. `git clone https://github.com/[your username]/OpenSearch.git`.

### Install Prerequisites

#### JDK 11

OpenSearch builds using Java 11 at a minimum. This means you must have a JDK 11 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 11 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-11`.

Download Java 11 from [here](https://adoptium.net/releases.html?variant=openjdk11).

#### JDK 14

To run the full suite of tests, download and install [JDK 14](https://jdk.java.net/archive/) and set `JAVA11_HOME`, and `JAVA14_HOME`. They are required by the [backwards compatibility test](./TESTING.md#testing-backwards-compatibility).

#### Runtime JDK

By default, the test tasks use bundled JDK runtime, configured in `buildSrc/version.properties` and set to JDK 17 (LTS). Other kind of test tasks (integration, cluster, ... ) use the same runtime as `JAVA_HOME`. However, the build supports compiling with JDK 11 and testing on a different version of JDK runtime. To do this, set `RUNTIME_JAVA_HOME` pointing to the Java home of another JDK installation, e.g. `RUNTIME_JAVA_HOME=/usr/lib/jvm/jdk-14`. Alternatively, the runtime JDK version could be provided as the command line argument, using combination of `runtime.java=<major JDK version>` property and `JAVA<major JDK version>_HOME` environment variable, for example `./gradlew -Druntime.java=17 ...` (in this case, the tooling expects `JAVA17_HOME` environment variable to be set).

#### Windows

On Windows, set `_JAVA_OPTIONS: -Xmx4096M`. You may also need to set `LongPathsEnabled=0x1` under `Computer\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem`.

#### Docker

Download and install [Docker](https://docs.docker.com/install/), required for building OpenSearch artifacts, and executing certain test suites.

On Windows, [use Docker Desktop 3.6](https://docs.docker.com/desktop/windows/release-notes/3.x/). See [OpenSearch#1425](https://github.com/opensearch-project/OpenSearch/issues/1425) for workarounds and issues with Docker Desktop 4.1.1.

### Build

To build all distributions of OpenSearch, run:

```
./gradlew assemble
```

To build a distribution to run on your local platform, run:

```
./gradlew localDistro
```

All distributions built will be under `distributions/archives`.

### Run Tests

OpenSearch uses a Gradle wrapper for its build. Run `gradlew` on Unix systems, or `gradlew.bat` on Windows in the root of the repository.

Start by running the test suite with `gradlew check`. This should complete without errors.

```
./gradlew check

=======================================
OpenSearch Build Hamster says Hello!
  Gradle Version        : 6.6.1
  OS Info               : Linux 5.4.0-1037-aws (amd64)
  JDK Version           : 11 (JDK)
  JAVA_HOME             : /usr/lib/jvm/java-11-openjdk-amd64
=======================================

...

BUILD SUCCESSFUL in 14m 50s
2587 actionable tasks: 2450 executed, 137 up-to-date
```

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

```bash
curl localhost:9200

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

Use `-Dtests.opensearch.` to pass additional settings to the running instance. For example, to enable OpenSearch to listen on an external IP address pass `-Dtests.opensearch.http.host`. Make sure your firewall or security policy allows external connections for this to work.

```bash
./gradlew run -Dtests.opensearch.http.host=0.0.0.0
```

## Use an Editor

### IntelliJ IDEA

When importing into IntelliJ you will need to define an appropriate JDK. The convention is that **this SDK should be named "11"**, and the project import will detect it automatically. For more details on defining an SDK in IntelliJ please refer to [this documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk). Note that SDK definitions are global, so you can add the JDK from any project, or after project import. Importing with a missing JDK will still work, IntelliJ will report a problem and will refuse to build until resolved.

You can import the OpenSearch project into IntelliJ IDEA as follows.

1. Select **File > Open**
2. In the subsequent dialog navigate to the root `build.gradle` file
3. In the subsequent dialog select **Open as Project**

#### Remote development using JetBrains Gateway

[JetBrains Gateway](https://www.jetbrains.com/remote-development/gateway/) enables development, testing and debugging on remote machines like development servers.

1. On the local development machine, download and install the latest thin client from the [JetBrains Gateway page](https://www.jetbrains.com/remote-development/gateway/).
2. Create a new connection to the remote server and install an IntelliJ server support using [these instructions](https://www.jetbrains.com/help/idea/remote-development-starting-page.html#connect_to_rd_ij).

Follow the [IntelliJ IDEA instructions](#intellij-idea) post a successful connection.

### Visual Studio Code

Follow links in the [Java Tutorial](https://code.visualstudio.com/docs/java/java-tutorial) to install the coding pack and extensions for Java, Gradle tasks, etc. Open the source code directory.

### Eclipse

When importing to Eclipse, you need to have [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) plugin installed and, preferably, have JDK 11 set as default JRE in **Preferences -> Java -> Installed JREs**. Once this is done, generate Eclipse projects using Gradle wrapper:

    ./gradlew eclipse

You can now import the OpenSearch project into Eclipse as follows.

1. Select **File > Import -> Existing Gradle Project**
2. In the subsequent dialog navigate to the root of `build.gradle` file
3. In the subsequent dialog, if JDK 11 is not set as default JRE, please make sure to check **[Override workspace settings]**, keep **[Gradle Wrapper]** and provide the correct path to JDK11 using **[Java Home]** property under **[Advanced Options]**. Otherwise, you may run into cryptic import failures and only top level project is going to be imported.
4. In the subsequent dialog, you should see **[Gradle project structure]** populated, please click **[Finish]** to complete the import

**Note:** it may look non-intuitive why one needs to use Gradle wrapper and then import existing Gradle project (in general, **File > Import -> Existing Gradle Project** should be enough). Practically, as it stands now, Eclipse Buildship plugin does not import OpenSearch project dependencies correctly but does work in conjunction with Gradle wrapper.

## Project Layout

This repository is split into many top level directories. The most important ones are:

### `distribution`

Builds our tar and zip archives and our rpm and deb packages.

### `libs`

Libraries used to build other parts of the project. These are meant to be internal rather than general purpose. We have no plans to
[semver](https://semver.org/) their APIs or accept feature requests for them. We publish them to maven central because they are dependencies of our plugin test framework, high level rest client, and jdbc driver but they really aren't general purpose enough to *belong* in maven central. We're still working out what to do here.

### `modules`

Features that are shipped with OpenSearch by default but are not built in to the server. We typically separate features from the server because they require permissions that we don't believe *all* of OpenSearch should have or because they depend on libraries that we don't believe *all* of OpenSearch should depend on.

For example, reindex requires the `connect` permission so it can perform reindex-from-remote but we don't believe that the *all* of OpenSearch should have the "connect". For another example, Painless is implemented using antlr4 and asm and we don't believe that *all* of OpenSearch should have access to them.

### `plugins`

OpenSearch plugins. We decide that a feature should be a plugin rather than shipped as a module because we feel that it is only important to a subset of users, especially if it requires extra dependencies.

The canonical example of this is the ICU analysis plugin. It is important for folks who want the fairly language neutral ICU analyzer but the library to implement the analyzer is 11MB so we don't ship it with OpenSearch by default.

Another example is the `discovery-gce` plugin. It is *vital* to folks running in [GCP](https://cloud.google.com/) but useless otherwise and it depends on a dozen extra jars.

### `sandbox`

This is where the community can add experimental features in to OpenSearch. There are three directories inside the sandbox - `libs`, `modules` and `plugins` - which mirror the subdirectories in the project root and have the same guidelines for deciding on where a new feature goes. The artifacts from `libs` and `modules` will be automatically included in the **snapshot** distributions. Once a certain feature is deemed worthy to be included in the OpenSearch release, it will be promoted to the corresponding subdirectory in the project root. **Note**: The sandbox code do not have any other guarantees such as backwards compatibility or long term support and can be removed at any time.

To exclude the modules from snapshot distributions, use the `sandbox.enabled` system property.

    ./gradlew assemble -Dsandbox.enabled=false

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

IntelliJ IDEs can [import](https://blog.jetbrains.com/idea/2014/01/intellij-idea-13-importing-code-formatter-settings-from-eclipse/) the [settings file](buildSrc/formatterConfig.xml), and / or use the [Eclipse Code Formatter](https://plugins.jetbrains.com/plugin/6546-eclipse-code-formatter)
plugin.

You can also tell Spotless to [format a specific file](https://github.com/diffplug/spotless/tree/master/plugin-gradle#can-i-apply-spotless-to-specific-files) from the command line.

### Formatting Failures

Sometimes Spotless will report a "misbehaving rule which can't make up its mind" and will recommend enabling the `paddedCell()` setting. If you enabled this settings and run the format check again, Spotless will write files to `$PROJECT/build/spotless-diagnose-java/` to aid diagnosis. It writes different copies of the formatted files, so that you can see how they
differ and infer what is the problem.

The `paddedCell()` option is disabled for normal operation in order to detect any misbehaviour. You can enable the option from the command line by running Gradle with `-Dspotless.paddedcell`.

> Note: if you have imported the project into IntelliJ IDEA the project will be automatically configured to add the correct license header to new source files based on the source location.

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

Security is our top priority. Avoid checking in credentials.

#### Installation
Install [awslabs/git-secrets](https://github.com/awslabs/git-secrets) by running the following commands.
```
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install
```

#### Configuration
You can configure git secrets per repository, you need to change the directory to the root of the repository and run the following command.
```
git secrets --install
✓ Installed commit-msg hook to .git/hooks/commit-msg
✓ Installed pre-commit hook to .git/hooks/pre-commit
✓ Installed prepare-commit-msg hook to .git/hooks/prepare-commit-msg
```
Then, you need to apply patterns for git-secrets, you can install the AWS standard patterns by running the following command.
```
git secrets --register-aws
```

## Components
As you work in the OpenSearch repo you may notice issues getting labeled with component labels.  It's a housekeeping task to help group together similar pieces of work.  You can pretty much ignore it, but if you're curious, here's what the different labels mean:

### Build libraries & interfaces
Tasks to make sure the build tasks are useful and packaging and distribution are easy.

Includes:

- Gradle for the Core tasks
- Groovy scripts
- build-tools
- Versioning interfaces
- Compatibility
- Javadoc enforcement


### Clients & Libraries
APIs and communication mechanisms for external connections to OpenSearch.  This includes the “library” directory in OpenSearch (a set of common functions).

Includes:

- Transport layer
- High Level and low level Rest Client
- CLI

### Plugins
Anything touching the plugin infrastructure within core OpenSearch.

Includes:

- API
- SPI
- Plugin interfaces


### Indexing & search
The critical path of indexing and search, including:  Measure index and search, performance, Improving the performance of indexing and search, ensure synchronization OpenSearch APIs with upstream Lucene change (e.g. new field types, changing doc values and codex).

Includes:

- Lucene Structures
- FieldMappers
- QueryBuilders
- DocValues

### Aggregations
Making sure OpenSearch can be used as a compute engine.

Includes:

- APIs (suggest supporting a formal API)
- Framework

### Distributed Framework
Work to make sure that OpenSearch can scale in a distributed manner.

Includes:

- Nodes (Master, Data, Compute, Ingest, Discovery, etc.)
- Replication & Merge Policies (Document, Segment level)
- Snapshot/Restore (repositories; S3, Azure, GCP, NFS)
- Translog (e.g., OpenSearch, Kafka, Kinesis)
- Shard Strategies
- Circuit Breakers

## Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).

## Backports

The Github workflow in [`backport.yml`](.github/workflows/backport.yml) creates backport PRs automatically when the original PR with an appropriate label `backport <backport-branch-name>` is merged to main with the backport workflow run successfully on the PR. For example, if a PR on main needs to be backported to `1.x` branch, add a label `backport 1.x` to the PR and make sure the backport workflow runs on the PR along with other checks. Once this PR is merged to main, the workflow will create a backport PR to the `1.x` branch.
