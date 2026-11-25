# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


WORKFLOW = Path(__file__).resolve().parents[1] / "workflows" / "version.yml"


def step_script(name):
    lines = WORKFLOW.read_text().splitlines()
    start = lines.index(f"      - name: {name}")
    if lines[start + 1] != "        run: |":
        raise ValueError(f"{name} is not a run step")
    body = []
    for line in lines[start + 2 :]:
        if not line.startswith("          "):
            break
        body.append(line[10:])
    return "\n".join(body)


class VersionWorkflowTests(unittest.TestCase):
    def version_info(self, tag, directory):
        env_file = directory / "github_env"
        env_file.write_text("")
        env = os.environ.copy()
        env.update(GITHUB_REF=f"refs/tags/{tag}", GITHUB_ENV=str(env_file))
        result = subprocess.run(
            ["bash", "-eo", "pipefail", "-c", step_script("Fetch Tag and Version Information")],
            cwd=directory,
            env=env,
            text=True,
            capture_output=True,
        )
        values = dict(line.split("=", 1) for line in env_file.read_text().splitlines())
        return result, values

    def test_version_ids_for_single_and_double_digit_components(self):
        cases = [
            ("1.3.8", "1.3.9", "1030999"),
            ("2.9.9", "2.9.10", "2091099"),
            ("2.19.8", "2.19.9", "2190999"),
            ("2.19.9", "2.19.10", "2191099"),
        ]
        with tempfile.TemporaryDirectory() as temp:
            for tag, next_version, expected_id in cases:
                with self.subTest(tag=tag):
                    result, values = self.version_info(tag, Path(temp))
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(values["NEXT_VERSION"], next_version)
                    self.assertEqual(values["NEXT_VERSION_ID"], expected_id)

    def test_invalid_or_unrepresentable_version_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            for tag in ("2.19.bad", "2.19.99", "2.019.8", "2.100.8"):
                with self.subTest(tag=tag):
                    result, values = self.version_info(tag, Path(temp))
                    self.assertNotEqual(result.returncode, 0)
                    self.assertTrue(result.stderr)
                    self.assertEqual(values, {})

    def test_patch_step_updates_catalog_and_version_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            files = {
                ".ci/bwcVersions": 'BWC_VERSION:\n  - "2.19.7"\n',
                "buildSrc/version.properties": "opensearch        = 2.19.8\n",
                "gradle/libs.versions.toml": '[versions]\nopensearch        = "2.19.8"\nlucene = "9.12.3"\n',
                "libs/core/src/main/java/org/opensearch/Version.java": (
                    "    public static final Version V_2_19_8 = new Version(2190899, "
                    "org.apache.lucene.util.Version.LUCENE_9_12_3);\n"
                    "    public static final Version CURRENT = V_2_19_8;\n"
                ),
            }
            for name, content in files.items():
                path = directory / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)

            info_result, values = self.version_info("2.19.8", directory)
            self.assertEqual(info_result.returncode, 0, info_result.stderr)
            env = os.environ.copy()
            env.update(values)
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", step_script("Increment Patch Version")],
                cwd=directory,
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                (directory / "gradle/libs.versions.toml").read_text(),
                '[versions]\nopensearch        = "2.19.9"\nlucene = "9.12.3"\n',
            )
            self.assertEqual((directory / "buildSrc/version.properties").read_text(), "opensearch        = 2.19.9\n")
            self.assertEqual((directory / ".ci/bwcVersions").read_text(), 'BWC_VERSION:\n  - "2.19.7"\n  - "2.19.8"\n')
            version_java = (directory / "libs/core/src/main/java/org/opensearch/Version.java").read_text()
            self.assertIn("V_2_19_9 = new Version(2190999, org.apache.lucene.util.Version.LUCENE_9_12_3)", version_java)
            self.assertIn("CURRENT = V_2_19_9;", version_java)

            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", step_script("Increment Patch Version")],
                cwd=directory,
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Version catalog does not contain expected", result.stderr)
            self.assertEqual((directory / ".ci/bwcVersions").read_text(), 'BWC_VERSION:\n  - "2.19.7"\n  - "2.19.8"\n')

            for name, content in files.items():
                (directory / name).write_text(content)
            version_path = directory / "libs/core/src/main/java/org/opensearch/Version.java"
            version_path.write_text(files[str(version_path.relative_to(directory))].replace("CURRENT = V_2_19_8", "CURRENT = V_2_19_7"))
            result = subprocess.run(
                ["bash", "-eo", "pipefail", "-c", step_script("Increment Patch Version")],
                cwd=directory,
                env=env,
                text=True,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Version.java does not declare the expected current version", result.stderr)
            self.assertEqual((directory / ".ci/bwcVersions").read_text(), files[".ci/bwcVersions"])
            self.assertEqual((directory / "gradle/libs.versions.toml").read_text(), files["gradle/libs.versions.toml"])


if __name__ == "__main__":
    unittest.main()
