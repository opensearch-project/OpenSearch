## ECS mappings generator

This script generates the ECS mappings for the Wazuh indices.

### Requirements

- ECS repository clone. The script is meant to be launched from the root level of that repository.
- `Python` 3.6 or higher + `venv` module
- `jq`

### Folder structure

There is a folder for each module. Inside each folder, there is a `fields` folder with the required
files to generate the mappings. These are the inputs for the ECS generator.

### Usage

1. Get a copy of the ECS repository at the same level as the `wazuh-indexer` repo:

    ```console
    git clone git@github.com:elastic/ecs.git
    ```

2. Install the dependencies:

    ```console
    cd ecs
    python3 -m venv env
    source env/bin/activate
    pip install -r scripts/requirements.txt
    ```

2. Copy the `generate.sh` script to the root level of the ECS repository.

    ```console
    cp generate.sh ../../ecs
    cd ../../ecs
    bash generate.sh
    ```

    Expected output:
    ```
    Usage: generate.sh <ECS_VERSION> <INDEXER_SRC> <MODULE> [--upload <URL>]
      * ECS_VERSION: ECS version to generate mappings for
      * INDEXER_SRC: Path to the wazuh-indexer repository
      * MODULE: Module to generate mappings for
      * --upload <URL>: Upload generated index template to the OpenSearch cluster. Defaults to https://localhost:9200
    Example: generate.sh v8.10.0 ~/wazuh-indexer vulnerability-detector --upload https://indexer:9200
    ```

3. Use the `generate.sh` script to generate the mappings for a module. The script takes 3 arguments,
plus 2 optional arguments to upload the mappings to the `wazuh-indexer`. Both, composable and legacy mappings
are generated. For example, to generate the mappings for the `vulnerability-detector` module using the
    ECS version `v8.10.0` and assuming that path of this repository is `~/wazuh/wazuh-indexer`:

    ```bash
    ./generate.sh v8.10.0 ~/wazuh/wazuh-indexer vulnerability-detector
    ```

    The tool will output the folder where they have been generated.

    ```console
    Loading schemas from git ref v8.10.0
    Running generator. ECS version 8.10.0
    Replacing "match_only_text" type with "text"
    Mappings saved to ~/wazuh/wazuh-indexer/ecs/vulnerability-detector/mappings/v8.10.0
    ```

4. When you are done. Exit the virtual environment.

    ```console
    deactivate
    ```

### Output

A new `mappings` folder will be created inside the module folder, containing all the generated files.
The files are versioned using the ECS version, so different versions of the same module can be generated.
For our use case, the most important files are under `mappings/<ECS_VERSION>/generated/elasticsearch/legacy/`:

- `template.json`: Elasticsearch compatible index template for the module
- `opensearch-template.json`: OpenSearch compatible index template for the module

The original output is `template.json`, which is not compatible with OpenSearch by default. In order
to make this template compatible with OpenSearch, the following changes are made:

- The `order` property is renamed to `priority`.
- The `mappings` and `settings` properties are nested under the `template` property.

The script takes care of these changes automatically, generating the `opensearch-template.json` file as a result.

### Upload

You can either upload the index template using cURL or the UI (dev tools).

```bash
curl -u admin:admin -k -X PUT "https://indexer:9200/_index_template/wazuh-vulnerability-detector" -H "Content-Type: application/json" -d @opensearch-template.json
```

Notes:
- PUT and POST are interchangeable.
- The name of the index template does not matter. Any name can be used.
- Adjust credentials and URL accordingly.

### Adding new mappings

The easiest way to create mappings for a new module is to take a previous one as a base.
Copy a folder and rename it to the new module name. Then, edit the `fields` files to
match the new module fields.

The name of the folder will be the name of the module to be passed to the script. All 3 files
are required.

- `fields/subset.yml`: This file contains the subset of ECS fields to be used for the module.
- `fields/template-settings-legacy.json`: This file contains the legacy template settings for the module.
- `fields/template-settings.json`: This file contains the composable template settings for the module.

### Event generator

For testing purposes, the script `generate_events.py` can be used to generate events for a given module.
Currently, it is only able to generate events for the `vulnerability-detector` module. To support other
modules, please extend of refactor the script.

The script prompts for the required parameters, so it can be launched without arguments:
  
```bash
./event_generator.py
```

The script will generate a JSON file with the events, and will also ask whether to upload them to the
indexer. If the upload option is selected, the script will ask for the indexer URL and port, credentials,
and index name.

The script uses log file. Check it out for debugging or additional information.

#### References

- [ECS repository](https://github.com/elastic/ecs)
- [ECS usage](https://github.com/elastic/ecs/blob/main/USAGE.md)
- [ECS field reference](https://www.elastic.co/guide/en/ecs/current/ecs-field-reference.html)
