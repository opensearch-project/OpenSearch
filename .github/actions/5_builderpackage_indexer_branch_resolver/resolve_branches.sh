#!/bin/bash
# Determines the correct branch for each repo based on input and VERSION.json

set -e

REPOS=(
    "wazuh-indexer-plugins"
    "wazuh-indexer-reporting"
    "wazuh-indexer-security-analytics"
    "wazuh"
)
REPO_URLS=(
    "https://github.com/wazuh/wazuh-indexer-plugins.git"
    "https://github.com/wazuh/wazuh-indexer-reporting.git"
    "https://github.com/wazuh/wazuh-indexer-security-analytics.git"
    "https://github.com/wazuh/wazuh.git"
)

declare -A BRANCH_EXISTS
declare -A VERSION

extract_version_branch() {
    local version="$1"
    [ "$version" == "5.0.0" ] && echo "main" || echo "$version"
}

navigate_to_project_root() {
    local repo_root_marker=".github"
    local script_path
    script_path="$(dirname "$(realpath "$0")")"
    while [[ "$script_path" != "/" ]] && [[ ! -d "$script_path/$repo_root_marker" ]]; do
        script_path="$(dirname "$script_path")"
    done
    cd "$script_path"
}

extract_current_repo_version() {
    if [[ -f VERSION.json ]]; then
        jq -r '.version' VERSION.json 2>/dev/null
    else
        echo ""
    fi
}

check_branch_existence() {
    for i in "${!REPOS[@]}"; do
        local repo="${REPOS[$i]}"
        local url="${REPO_URLS[$i]}"
        echo "Checking $repo for branch '$BRANCH'..." >&2
        if git ls-remote --exit-code --heads "$url" "$BRANCH" &>/dev/null; then
            BRANCH_EXISTS["$repo"]=1
            # Fetch VERSION.json directly from GitHub raw URL
            local owner version_json_url version
            owner="wazuh"
            version_json_url="https://raw.githubusercontent.com/${owner}/${repo}/${BRANCH}/VERSION.json"
            version=$(curl -sfL "$version_json_url" | jq -r '.version' 2>/dev/null)
            if [[ -n "$version" && "$version" != "null" ]]; then
                VERSION["$repo"]="$version"
                echo "    Found branch and version: $version" >&2
            else
                VERSION["$repo"]=""
                echo "    Found branch but VERSION.json not found!" >&2
            fi
        else
            BRANCH_EXISTS["$repo"]=0
            VERSION["$repo"]=""
            echo "    Branch not found." >&2
        fi
    done
}

get_branches_to_use() {
    local all_missing=1
    for repo in "${REPOS[@]}"; do
        if [[ "${BRANCH_EXISTS[$repo]}" -eq 1 ]]; then
            all_missing=0
            echo "${repo}=${BRANCH}"
        fi
    done

    local current_version
    current_version=$(extract_current_repo_version)
    if [[ $all_missing -eq 1 ]]; then
        echo "Branch '$BRANCH' not found in any repo, using current repo's reference." >&2
        if [[ -n "$current_version" && "$current_version" != "null" ]]; then
            local fallback_branch
            fallback_branch=$(extract_version_branch "$current_version")
            for repo in "${REPOS[@]}"; do
                echo "${repo}=${fallback_branch}"
            done
        else
            echo "ERROR: Branch '$BRANCH' not found in any repo and no VERSION.json in current repo." >&2
            exit 1
        fi
    else
        for repo in "${REPOS[@]}"; do
            if [[ "${BRANCH_EXISTS[$repo]}" -eq 0 ]]; then
                local fallback_version=""
                for other_repo in "${REPOS[@]}"; do
                    if [[ "$other_repo" != "$repo" && "${VERSION[$other_repo]}" != "" ]]; then
                        fallback_version="${VERSION[$other_repo]}"
                        break
                    fi
                done
                if [[ -n "$fallback_version" ]]; then
                    local fallback_branch
                    fallback_branch=$(extract_version_branch "$fallback_version")
                    echo "${repo}=${fallback_branch}"
                elif [[ -n "$current_version" && "$current_version" != "null" ]]; then
                    local fallback_branch
                    fallback_branch=$(extract_version_branch "$current_version")
                    echo "${repo}=${fallback_branch}"
                else
                    echo "ERROR: Branch '$BRANCH' not found in $repo and no fallback version available." >&2
                    exit 1
                fi
            fi
        done
    fi
}

main() {
    local BRANCH="$1"
    if [[ -z "$BRANCH" ]]; then
        echo "Usage: $0 <branch-name>"
        exit 1
    fi
    navigate_to_project_root
    check_branch_existence
    get_branches_to_use
}

main "$@"
