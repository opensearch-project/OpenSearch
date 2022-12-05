- [Developer Guide](#developer-guide)
    - [Forking and Cloning](#forking-and-cloning)
    - [Install Prerequisites](#install-prerequisites)
        - [JDK 11](#jdk-11)
    - [Setup](#setup)
    - [Build](#build)
        - [Building from the command line](#building-from-the-command-line)
        - [Debugging](#debugging)
    - [Using IntelliJ IDEA](#using-intellij-idea)
    - [Submitting Changes](#submitting-changes)

## Developer Guide

So you want to contribute code to this project? Excellent! We're glad you're here. Here's what you need to do.

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 11

OpenSearch components build using Java 11 at a minimum. This means you must have a JDK 11 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 11 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-11`.

## Setup

1. Check out this package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.
3. To build from the command line, set `JAVA_HOME` to point to a JDK >= 11 before running `./gradlew`.
- Unix System
    1. `export JAVA_HOME=jdk-install-dir`: Replace `jdk-install-dir` with the JAVA_HOME directory of your system.
    2. `export PATH=$JAVA_HOME/bin:$PATH`

- Windows System
    1. Find **My Computers** from file directory, right click and select **properties**.
    2. Select the **Advanced** tab, select **Environment variables**.
    3. Edit **JAVA_HOME** to path of where JDK software is installed.

## Build
The JobScheduler plugin uses the [Gradle](https://docs.gradle.org/4.10.2/userguide/userguide.html)
build system.
1. Checkout this package from version control.
1. To build from command line set `JAVA_HOME` to point to a JDK >=11
1. Run `./gradlew build`

Then you will find the built artifact located at `build/distributions` directory

## Install
Once you have built the plugin from source code, run
```bash
opensearch-plugin install file:///path/to/target/releases/opensearch-job-scheduler-<version>.zip
```
to install the JobScheduler plugin to your OpenSearch.

## Develop a plugin that extends JobScheduler
JobScheduler plugin provides a SPI for other plugins to implement. Essentially, you need to
1. Define your *JobParameter* type by implementing `ScheduledJobParameter` interface
1. Implement your JobParameter parser function that can deserialize your JobParameter from XContent
1. Create your *JobRunner* implementation by implementing `ScheduledJobRunner` interface
1. Create your own plugin which implements `JobSchedulerExtension` interface
    - don't forget to create the service provider configuration file in your resources folder and
      bundle it into your plugin artifact

Please refer to the `sample-extension-plugin` subproject in this project, which provides a complete
example of using JobScheduler to run periodical jobs.

The sample extension plugin takes an index name as input and logs the index shards to opensearch
logs according to the specified Schedule. And it also exposes a REST endpoint for end users to
create/delete jobs.


### Using IntelliJ IDEA

Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.

### Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).

### Backport

- [Link to backport documentation](https://github.com/opensearch-project/opensearch-plugins/blob/main/BACKPORT.md)