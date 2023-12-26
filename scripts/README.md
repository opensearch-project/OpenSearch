# `wazuh-indexer` packages generation guide

The packages' generation process consists on 2 steps:

* **Build**: compiles the Java application and bundles it into a package.
* **Assembly**: uses the package from the previous step and inflates it with plugins and 
configuration files, ready for production deployment.

We usually generate the packages using GitHub Actions, however, the process is designed to
be independent enough for maximum portability. GitHub Actions provides infrastructure, while
the building process is self-contained in the application code.

Each section includes instructions to generate packages locally, using Act or Docker.

- [Install Act](https://github.com/nektos/act)

## Build

...
...

#### Act (GitHub Workflow locally)

```console
act -j build -W .github/workflows/build.yml --artifact-server-path ./artifacts

[Build slim packages/build] 🏁  Job succeeded
```


#### Running in Docker

Within the [Docker environment](../docker):

```console
bash scripts/build.sh -v 2.11.0 -s false -p linux -a {x64|arm64} -d {rpm|deb|tar}
```

The generated package is sent to `artifacts/`


## Assemble

<!-- 
### TAR
### DEB
-->

### RPM

The `assemble.sh` script will use the output from the `build.sh` script and use it as a 
base to bundle together a final package containing the plugins, the production configuration 
and the service files.

The script will:

- Extract the rpm package using `rpm2cpio` and `cpio` tools.
    
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

    `usr`, `etc` and `var` folders contain `wazuh-indexer` files, extracted from `wazuh-indexer-min-*.rpm`.
    `wazuh-indexer.rpm.spec` is copied over from `wazuh-indexer/distribution/packages/src/rpm/wazuh-indexer.rpm.spec`. 
    The `wazuh-indexer-performance-analyzer.service` file is also copied from the same folder. It is a dependency of the SPEC file.

- Install the plugins using the `opensearch-plugin` CLI tool.
- Set up configuration files.

    > Included in `min-package`. Default files are overwritten.
    
- Bundle an RPM file with `rpmbuild` and the SPEC file `wazuh-indexer.rpm.spec`.
    - `rpmbuild` is part of the `rpm` OS package.

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

### Running in Act

```console
act -j assemble -W .github/workflows/build.yml --artifact-server-path ./artifacts --matrix distribution:rpm --matrix architecture:x64 --var OPENSEARCH_VERSION=2.11.0

[Build slim packages/build] 🏁  Job succeeded
```

#### Running in Docker

Pre-requisites:

* Current directory: `wazuh-indexer/`
* Existing rpm package in `wazuh-indexer/artifacts/dist/rpm`, as a result of the _Build_ stage.

```console
MIN_PKG_PATH="./artifacts"
docker run --rm \
    -v ./scripts/:/home/wazuh-indexer/scripts \
    -v $MIN_PKG_PATH:/home/wazuh-indexer/artifacts \
    -v ./distribution/packages/src:/home/wazuh-indexer/distribution/packages/src \
    -w /home/wazuh-indexer \
    -it ubuntu:jammy /bin/bash

apt-get update
apt-get install -y rpm2cpio rpm cpio
bash scripts/assemble.sh -v 2.11.0 -p linux -a x64 -d rpm
```

