# Wazuh Indexer build scripts and flow reference

## Introduction

The packages' generation process consists on 2 steps:

- **Build**: compiles the Java application and bundles it into a package.
- **Assembly**: uses the package from the previous step and inflates it with plugins and configuration files, ready for production deployment.

We usually generate the packages using GitHub Actions, however, the process is designed to be independent enough for maximum portability. GitHub Actions provides infrastructure, while the building process is self-contained in the application code.

This guide includes development worthy information about the process to generate packages for Wazuh Indexer. For instructions about how to build Wazuh Indexer packages, refer to the instructions in the [README.md](./README.md).

## Scripts reference

There is a script for each of the steps, `build.sh` and `assemble.sh`. Along them, there are some utility scripts, such as `provision.sh` and `baptizer.sh`. This one generates the names for the packages depending on the combination of parameters given. Refer to the definition below for more details about these scripts.

```yml
scripts:
  - file: build.sh
    description: |
      Generates a distribution package by running the appropiate Gradle task
      depending on the parameters.
    inputs:
      architecture: [x64, arm64]
      distribution: [tar, deb, rpm]
      name: the name of the package to be generated.
    outputs:
      package: minimal wazuh-indexer package for the given distribution and architecture.

  - file: assemble.sh
    description: |
      Bundles the wazuh-indexer package generated in by build.sh with plugins,
      configuration files and demo certificates (certificates yet to come).
    inputs:
      architecture: [x64, arm64]
      distribution: [tar, deb, rpm]
      revision: revision number. 0 by default.
    outputs:
      package: final wazuh-indexer package.

  - file: provision.sh
    description: Provision script for the assembly of Debian packages.

  - file: baptizer.sh
    description: Generates the wazuh-indexer package name depending on the parameters.
    inputs:
      architecture: [x64, arm64]
      distribution: [tar, deb, rpm]
      revision: revision number. 0 by default.
      plugins_hash: Commit hash of the `wazuh-indexer-plugins` repository.
      is_release: if set, uses release naming convention.
      is_min: if set, the package name will start by `wazuh-indexer-min`. Used on the build stage.
    outputs:
      package: the name of the wazuh-indexer package.
```

## Build and Assemble

### Build

The build process is identical for every distribution and architecture. Although the process is driven by the `build.sh` script, the compilation and bundling is performed by Gradle tasks.

```bash
./build.sh -h
Usage: ./build.sh [args]

Arguments:
-q QUALIFIER    [Optional] Version qualifier.
-s SNAPSHOT     [Optional] Build a snapshot, default is 'false'.
-p PLATFORM     [Optional] Platform, default is 'uname -s'.
-a ARCHITECTURE [Optional] Build architecture, default is 'uname -m'.
-d DISTRIBUTION [Optional] Distribution, default is 'tar'.
-b BRANCH       [Optional] Branch from wazuh/wazuh to download the index template from, default is $(bash build-scripts/product_version.sh)
-n NAME [optional] Package name, default is set automatically.
-o OUTPUT       [Optional] Output path, default is 'artifacts'.
-h help
```

### Assemble

The assemble process is mostly similar for every distribution, with small differences detailed below.

#### Tarball packages

The assembly process for tarballs consists on:

1. Extraction of the minimal packages.
2. Installation of the plugins.
3. Addition of Wazuh's configuration files and tools.
4. Bundling (tar).

For example:

```bash
bash build-scripts/assemble.sh -a x64 -d tar -r 1
```

#### Debian packages

For Debian packages, the `assemble.sh` script will perform the following operations:

1. Extract the deb package using `ar` and `tar` tools.

   > By default, `ar` and `tar` tools expect the package to be in `wazuh-indexer/artifacts/tmp/deb`.
   > The script takes care of creating the required folder structure, copying also the min package and the Makefile.

   Current folder loadout at this stage:

   ```
   artifacts/
   |-- dist
   |   |-- wazuh-indexer-min_<version>_amd64.deb
   `-- tmp
       `-- deb
           |-- Makefile
           |-- data.tar.gz
           |-- debmake_install.sh
           |-- etc
           |-- usr
           |-- var
           `-- wazuh-indexer-min_<version>_amd64.deb
   ```

   Notes:

   - `usr`, `etc` and `var` folders contain `wazuh-indexer` files, extracted from `wazuh-indexer-min-*.deb`.
   - `Makefile` and the `debmake_install` are copied over from `wazuh-indexer/distribution/packages/src/deb`.

2. Install the plugins using the `opensearch-plugin` CLI tool.
3. Set up configuration files.

   > Included in `min-package`. Default files are overwritten.

4. Bundle a DEB file with `debmake` and the `Makefile`.

   > `debmake` and other dependencies can be installed using the `provision.sh` script.
   > The script is invoked by the GitHub Workflow.

   Current folder loadout at this stage:

   ```
   artifacts/
   |-- artifact_name.txt
   |-- dist
   |   |-- wazuh-indexer-min_<version>_amd64.deb
   |   `-- wazuh-indexer_<version>_amd64.deb
   `-- tmp
       `-- deb
           |-- Makefile
           |-- data.tar.gz
           |-- debmake_install.sh
           |-- etc
           |-- usr
           |-- var
           |-- wazuh-indexer-min_<version>_amd64.deb
           `-- debian/
               | -- control
               | -- copyright
               | -- rules
               | -- preinst
               | -- prerm
               | -- postinst
   ```

#### RPM packages

For RPM packages, the `assemble.sh` script will:

1. Extract the RPM package using `rpm2cpio` and `cpio` tools.

   > By default, `rpm2cpio` and `cpio` tools expect the package to be in `wazuh-indexer/artifacts/tmp/rpm`. The script takes care of creating the required folder structure, copying also the min package and the SPEC file.

   Current folder loadout at this stage:

   ```
   /rpm/$ARCH
       /etc
       /usr
       /var
       wazuh-indexer-min-*.rpm
       wazuh-indexer.rpm.spec
   ```

   Notes:

   - `usr`, `etc` and `var` folders contain `wazuh-indexer` files, extracted from `wazuh-indexer-min-*.rpm`.
   - `wazuh-indexer.rpm.spec` is copied over from `wazuh-indexer/distribution/packages/src/rpm/wazuh-indexer.rpm.spec`.
   
2. Install the plugins using the `opensearch-plugin` CLI tool.
3. Set up configuration files.

   > Included in `min-package`. Default files are overwritten.

4. Bundle an RPM file with `rpmbuild` and the SPEC file `wazuh-indexer.rpm.spec`.

   > `rpmbuild` is part of the `rpm` OS package.

   > `rpmbuild` is invoked from `wazuh-indexer/artifacts/tmp/rpm`. It creates the {BUILD,RPMS,SOURCES,SRPMS,SPECS,TMP} folders and applies the rules in the SPEC file. If successful, `rpmbuild` will generate the package in the `RPMS/` folder. The script will copy it to `wazuh-indexer/artifacts/dist` and clean: remove the `tmp\` folder and its contents.

   Current folder loadout at this stage:

   ```
   /rpm/$ARCH
       /{BUILD,RPMS,SOURCES,SRPMS,SPECS,TMP}
       /etc
       /usr
       /var
       wazuh-indexer-min-*.rpm
       wazuh-indexer.rpm.spec
   ```

### Epilogue: Act

- [Install Act](https://github.com/nektos/act)

Use Act to run the `build.yml` workflow locally. The `act.input.env` file contains the inputs for the workflow. As the workflow clones the `wazuh-indexer-plugins` repository, the `GITHUB_TOKEN` is required. You can use the `gh` CLI to authenticate, as seen in the example below.

```bash
act -j build -W .github/workflows/build.yml --artifact-server-path ./artifacts --input-file build-scripts/act.input.env -s GITHUB_TOKEN="$(gh auth token)"
```
