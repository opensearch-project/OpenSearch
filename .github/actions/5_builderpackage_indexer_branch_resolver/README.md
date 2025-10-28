# Wazuh Indexer Branch Resolver Action

GitHub's limit on Workflow inputs is 10. As Wazuh Indexer is built out of multiple repositories, hitting this limit is more than a possible scenario. This action helps to reduce the number of inputs by automatically resolving the correct branches for dependent repositories based on the current branch and VERSION.json files.

By invoking this action, the workflows that build wazuh-indexer packages can simplify their input parameters and ensure that the correct branches are used for all dependent repositories. This criteria is based on the following decision tree:

```
1. Does the input branch exist in the repository?
   ├─ YES → Use that branch
   └─ NO → Continue to step 2

2. Does the input branch exist in any other repository?
   ├─ YES → Read the product version from that repository's branch (present in VERSION.json)
   │        └─ Use version-based branch on repositories missing the input branch (e.g., 5.0.0 → main, 4.12.1 → 4.12.1)
   └─ NO → Continue to step 3

3. Does VERSION.json exist in the current repository?
   ├─ YES → Use version-based branch
   └─ NO → ERROR: No fallback available
```

> **Note:** Currently, only `5.0.0` maps to `main`. All other versions map directly to their full version string.

## Usage

### Requirements

- **bash**: Shell scripting
- **jq**: JSON parsing
- **git**: Repository operations
- **curl**: Fetching VERSION.json from GitHub
- **awk**: Text processing

All dependencies are available in standard GitHub Actions runners.

### Inputs

| Input    | Description                      | Required | Default |
| -------- | -------------------------------- | -------- | ------- |
| `branch` | Branch name to check and resolve | Yes      | -       |

### Outputs

| Output                                    | Description                                                           |
| ----------------------------------------- | --------------------------------------------------------------------- |
| `wazuh_indexer_plugins_branch`            | Resolved branch name for wazuh-indexer-plugins repository            |
| `wazuh_indexer_reporting_branch`          | Resolved branch name for wazuh-indexer-reporting repository          |
| `wazuh_indexer_security_analytics_branch` | Resolved branch name for wazuh-indexer-security-analytics repository |

The script outputs branch assignments in `key=value` format:

```
wazuh-indexer-plugins=feature-branch
wazuh-indexer-reporting=main
```

The action will fail with a non-zero exit code in the following scenarios:

1. No branch name provided
2. Branch not found in any repo and no VERSION.json available
3. Unable to parse VERSION.json files
4. Network errors when accessing remote repositories

Diagnostic messages are sent to stderr, allowing easy parsing of stdout.

### In a Workflow

```yaml
jobs:
  branches:
    runs-on: ubuntu-24.04
    outputs:
      wazuh_plugins_ref: ${{ steps.resolve.outputs.wazuh_indexer_plugins_branch }}
      reporting_plugin_ref: ${{ steps.resolve.outputs.wazuh_indexer_reporting_branch }}
    steps:
      - uses: actions/checkout@v5
      
      - name: Resolve branches
        id: resolve
        uses: ./.github/actions/5_builderpackage_indexer_branch_resolver
        with:
          branch: ${{ github.ref_name }}
      
      - name: Display resolved branches
        run: |
          echo "Plugins branch: ${{ steps.resolve.outputs.wazuh_indexer_plugins_branch }}"
          echo "Reporting branch: ${{ steps.resolve.outputs.wazuh_indexer_reporting_branch }}"
          echo "Security Analytics branch: ${{ steps.resolve.outputs.wazuh_indexer_security_analytics_branch }}"

  build-plugins:
    needs: [branches]
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v5
        with:
          repository: wazuh/wazuh-indexer-plugins
          ref: ${{ needs.branches.outputs.wazuh_plugins_ref }}
      
      - name: Build plugins
        run: ./gradlew build
```

### Locally

You can also run the underlying script directly from the command line:

```bash
bash .github/actions/5_builderpackage_indexer_branch_resolver/resolve_branches.sh feature-branch
```

## Examples

**Branch exists in every repository**
  - Scenario: input branch exists in all the repositories.
  - Input: *feature-xyz*
  - Output:
    ```
    wazuh-indexer-plugins=feature-xyz
    wazuh-indexer-reporting=feature-xyz
    wazuh-indexer-security-analytics=feature-xyz
    ```

**Branch exists in one of the repositories**

- Scenario: input branch exists in **wazuh-indexer-plugins** but not in **wazuh-indexer-reporting** and **wazuh-indexer-security-analytics**. Input branch is based on version *4.12.1*.
- Input: *feature-xyz*
- Output:
    ```
    wazuh-indexer-plugins=feature-xyz
    wazuh-indexer-reporting=4.12.1
    wazuh-indexer-security-analytics=4.12.1
    ```

**Branch doesn't exist in any repository**

- Scenario: input branch does not exist on any repository. The branch in **wazuh-indexer** (holding the main code) is based on version *4.13.1*.
- Input: *feature-xyz*
- Output:
    ```
    wazuh-indexer-plugins=4.13.1
    wazuh-indexer-reporting=4.13.1
    wazuh-indexer-security-analytics=4.13.1
    ```

## Adding new repositories

To add more dependent repositories, edit `resolve_branches.sh`:

```bash
REPOS=(
    "wazuh-indexer-plugins"
    "wazuh-indexer-reporting"
    "wazuh-indexer-security-analytics"
    "your-new-repo"  # Add here
)
REPO_URLS=(
    "https://github.com/wazuh/wazuh-indexer-plugins.git"
    "https://github.com/wazuh/wazuh-indexer-reporting.git"
    "https://github.com/wazuh/wazuh-indexer-security-analytics.git"
    "https://github.com/wazuh/your-new-repo.git"  # Add here
)
```

Then update `action.yml` to add corresponding outputs:

```yaml
outputs:
  your_new_repo_branch:
    description: "Branch for your-new-repo"
    value: ${{ steps.resolve.outputs.your_new_repo_branch }}
```

## Troubleshooting

To see detailed output when running locally:

```bash
bash -x .github/actions/5_builderpackage_indexer_branch_resolver/resolve_branches.sh feature-branch
```
