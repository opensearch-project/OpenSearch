# Template for creating OpenSearch Plugins
This Repo is a GitHub Template repository ([Learn more about that](https://docs.github.com/articles/creating-a-repository-from-a-template/)).
Using it would create a new repo that is the boilerplate code required for an [OpenSearch Plugin](https://opensearch.org/blog/technical-posts/2021/06/my-first-steps-in-opensearch-plugins/). 
This plugin on its own would not add any functionality to OpenSearch, but it is still ready to be installed.
It comes packaged with:
 - Integration tests of two types: Yaml and IntegTest.
 - Empty unit tests file
 - Notice and License files (Apache License, Version 2.0)
 - A `build.gradle` file supporting this template's current state.

---
---
1. [Create your plugin repo using this template](#create-your-plugin-repo-using-this-template)
   - [Official plugins](#official-plugins)
   - [Thirdparty plugins](#thirdparty-plugins)
2. [Fix up the template to match your new plugin requirements](#fix-up-the-template-to-match-your-new-plugin-requirements)
   - [Plugin Name](#plugin-name)
   - [Plugin Path](#plugin-path)
   - [Update the `build.gradle` file](#update-the-buildgradle-file)
   - [Update the tests](#update-the-tests)
   - [Running the tests](#running-the-tests)
   - [Running testClusters with the plugin installed](#running-testclusters-with-the-plugin-installed)
   - [Cleanup template code](#cleanup-template-code)
   - [Editing the CI workflow](#Editing-the-CI-workflow)
3. [License](#license)
4. [Copyright](#copyright)
---
---

## Create your plugin repo using this template
Click on "Use this Template"

![Use this Template](https://docs.github.com/assets/images/help/repository/use-this-template-button.png)

Name the repository, and provide a description.

Depending on the plugin relationship with the OpenSearch organization we currently recommend the following naming conventions and optional follow-up checks:

### Official plugins

For the **official plugins** that live within the OpenSearch organization (i.e. they are included in [OpenSearch/plugins/](https://github.com/opensearch-project/OpenSearch/tree/main/plugins) or [OpenSearch/modules/](https://github.com/opensearch-project/OpenSearch/tree/main/modules) folder), and **which share the same release cycle as OpenSearch** itself:

- Do not include the word `plugin` in the repo name (e.g. [job-scheduler](https://github.com/opensearch-project/job-scheduler))
- Use lowercase repo names
- Use spinal case for repo names (e.g. [job-scheduler](https://github.com/opensearch-project/job-scheduler))
- Do not include the word `OpenSearch` or `OpenSearch Dashboards` in the repo name
- Provide a meaningful description, e.g. `An OpenSearch Dashboards plugin to perform real-time and historical anomaly detection on OpenSearch data`.

### Thirdparty plugins

For the **3rd party plugins** that are maintained as independent projects in separate GitHub repositories **with their own release cycles** the recommended naming convention should follow the same rules as official plugins with some exceptions and few follow-up checks:

- Inclusion of the words like `OpenSearch` or `OpenSearch Dashboard` (and in reasonable cases even `plugin`) are welcome because they can increase the chance of discoverability of the repository
- Check the plugin versioning policy is documented and help users know which versions of the plugin are compatible and recommended for specific versions of OpenSearch 
- Review [CONTRIBUTING.md](CONTRIBUTING.md) document which is by default tailored to the needs of Amazon Web Services developer teams. You might want to update or further customize specific parts related to:
  - **Code of Conduct** (if you do not already have CoC policy then there are several options to start with, such as [Contributor Covenant](https://www.contributor-covenant.org/)),
  - **Security Policy** (you should let users know how they can safely report security vulnerabilities),
  - Check if you need explicit part about **Trademarks and Attributions** (if you use any registered or non-registered trademarks we recommend following applicable "trademark-use" documents provided by respective trademark owners)

## Fix up the template to match your new plugin requirements

This is the file tree structure of the source code, as you can see there are some things you will want to change.

```
`-- src
    |-- main
    |   `-- java
    |       `-- org
    |           `-- example
    |               `-- path
    |                   `-- to
    |                       `-- plugin
    |                           `-- RenamePlugin.java
    |-- test
    |   `-- java
    |       `-- org
    |           `-- example
    |               `-- path
    |                   `-- to
    |                       `-- plugin
    |                           |-- RenamePluginIT.java
    |                           `-- RenameTests.java
    `-- yamlRestTest
        |-- java
        |   `-- org
        |       `-- example
        |           `-- path
        |               `-- to
        |                   `-- plugin
        |                       `-- RenameClientYamlTestSuiteIT.java
        `-- resources
            `-- rest-api-spec
                `-- test
                    `-- 10_basic.yml

```

### Plugin Name
Now that you have named the repo, you can change the plugin class `RenamePlugin.java` to have a meaningful name, keeping the `Plugin` suffix.
Change `RenamePluginIT.java`, `RenameTests.java`, and `RenameClientYamlTestSuiteIT.java` accordingly, keeping the `PluginIT`, `Tests`, and `ClientYamlTestSuiteIT` suffixes.

### Plugin Path 
You will need to change these paths in the source tree:

1) Package Path
    ```
    `-- org
        `-- example
    ```
    Let's call this our *package path*. In Java, package naming convention is to use a domain name in order to create a unique package name.
    This is normally your organization's domain.

2) Plugin Path
    ```
     `-- path
         `-- to
             `-- plugin
    ```
    Let's call this our *plugin path*, as the plugin class would be installed in OpenSearch under that path.
    This can be an existing path in OpenSearch, or it can be a unique path for your plugin. We recommend changing it to something meaningful.

3) Change all these path occurrences to match your chosen path and naming by following [this](#update-the-buildgradle-file) section

### Update the `build.gradle` file

Update the following section, using the new repository name and description, plugin class name, package and plugin paths:

```
def pluginName = 'rename'                // Can be the same as new repo name except including words `plugin` or `OpenSearch` is discouraged
def pluginDescription = 'Custom plugin'  // Can be same as new repo description
def packagePath = 'org.example'          // The package name for your plugin (convention is to use your organization's domain name)
def pathToPlugin = 'path.to.plugin'      // The path you chose for the plugin
def pluginClassName = 'RenamePlugin'     // The plugin class name
```

Next update the version of OpenSearch you want the plugin to be installed into. Change the following param:
```
    ext {
        opensearch_version = "1.0.0-beta1" // <-- change this to the version your plugin requires
    }
```

- Run `./gradlew preparePluginPathDirs` in the terminal
- Move the java classes into the new directories (will require to edit the `package` name in the files as well)
- Delete the old directories (the `org.example` directories)

### Update the tests
Notice that in the tests we are checking that the plugin was installed by sending a GET `/_cat/plugins` request to the cluster and expecting `rename` to be in the response.
In order for the tests to pass you must change `rename` in `RenamePluginIT.java` and in `10_basic.yml` to be the `pluginName` you defined in the `build.gradle` file in the previous section.

### Running the tests
You may need to install OpenSearch and build a local artifact for the integration tests and build tools ([Learn more here](https://github.com/opensearch-project/opensearch-plugins/blob/main/BUILDING.md)):

```
~/OpenSearch (main)> git checkout 1.0.0-beta1 -b beta1-release
~/OpenSearch (main)> ./gradlew publishToMavenLocal -Dbuild.version_qualifier=beta1 -Dbuild.snapshot=false
```

Now you can run all the tests like so:
```
./gradlew check
```

### Running testClusters with the plugin installed 
```
./gradlew run
```

Then you can see that your plugin has been installed by running: 
```
curl -XGET 'localhost:9200/_cat/plugins'
```

### Cleanup template code
- You can now delete the unused paths - `path/to/plugin`.
- Remove this from the `build.gradle`:

```
tasks.register("preparePluginPathDirs") {
    mustRunAfter clean
    doLast {
        def newPath = pathToPlugin.replace(".", "/")
        mkdir "src/main/java/$packagePath/$newPath"
        mkdir "src/test/java/$packagePath/$newPath"
        mkdir "src/yamlRestTest/java/$packagePath/$newPath"
    }
}
```

- Last but not least, add your own `README.md` instead of this one 

### Editing the CI workflow
You may want to edit the CI of your new repo.
  
In your new GitHub repo, head over to `.github/workflows/CI.yml`. This file describes the workflow for testing new push or pull-request actions on the repo.
Currently, it is configured to build the plugin and run all the tests in it.

You may need to alter the dependencies required by your new plugin.
Also, the **OpenSearch version** in the `Build OpenSearch` and in the `Build and Run Tests` steps should match your plugins version in the `build.gradle` file.

To view more complex CI examples you may want to checkout the workflows in official OpenSearch plugins, such as [anomaly-detection](https://github.com/opensearch-project/anomaly-detection/blob/main/.github/workflows/test_build_multi_platform.yml).

## Your Plugin's License
Source code files in this template contains the following header:
```
/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
  */
```
This plugin template is indeed open-sourced while you might choose to use it to create a proprietary plugin.
Be sure to update your plugin to meet any licensing requirements you may be subject to.

## License
This code is licensed under the Apache 2.0 License. See [LICENSE.txt](LICENSE.txt).

## Copyright
Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
