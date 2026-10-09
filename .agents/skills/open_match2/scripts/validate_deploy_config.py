#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Validates Open Match 2 deployment manifests for unreplaced placeholders and sample values."""

import argparse
from pathlib import Path
import re
import sys

UNREPLACED_PLACEHOLDERS = (
    "$SERVICE_ACCOUNT",
    "$PRIMARY_ENDPOINT",
    "$READ_ENDPOINT",
    "$GCP_SA_FOR_METRICS",
)

SAMPLE_HARDCODED_VALUES = {
    "peteryizhong-gke-dev": "Sample GCP project ID in deploy/gke/om.yaml googlemanagedprometheus exporter",
    "yi-standard-cluster": "Sample GKE cluster name in deploy/gke/om.yaml k8s.cluster.name resource attribute",
}

POSITIVE_REQUIRED_OM_INT_VARS = (
    "OM_CACHE_EXPIRATION_INTERVAL_MS",
    "OM_CACHE_IN_MAX_APPLY_DURATION_MS",
    "OM_CACHE_EXPIRATION_MAX_DELETES_PER_CYCLE",
    "OM_CACHE_IN_QUEUE_BUFFER_SIZE",
    "OM_CACHE_OUT_QUEUE_BUFFER_SIZE",
    "OM_MATCH_TICKET_DEACTIVATION_TIMEOUT_MS",
    "OM_CACHE_IN_MAX_UPDATES_PER_POLL",
    "OM_MAX_STATE_UPDATES_PER_CALL",
)


def audit_manifest(file_path: Path) -> list[str]:
    if not file_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"Path is not a regular file: {file_path}")

    issues: list[str] = []
    lines = file_path.read_text(encoding="utf-8").splitlines()

    for line_num, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        for placeholder in UNREPLACED_PLACEHOLDERS:
            if placeholder in stripped:
                issues.append(
                    f"{file_path}:{line_num}: Unreplaced template placeholder '{placeholder}' found: {stripped}"
                )

        for sample_val, explanation in SAMPLE_HARDCODED_VALUES.items():
            if sample_val in stripped:
                issues.append(
                    f"{file_path}:{line_num}: Hardcoded sample value '{sample_val}' ({explanation}): {stripped}"
                )

        for var_name in POSITIVE_REQUIRED_OM_INT_VARS:
            match = re.search(rf"\b{var_name}\b\s*[:=]\s*['\"]?(-?\d+)['\"]?", stripped)
            if match:
                val = int(match.group(1))
                if val <= 0:
                    issues.append(
                        f"{file_path}:{line_num}: '{var_name}' must be > 0 (got {val})"
                    )

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Audit Open Match 2 deployment YAML files (service.yaml, cloudbuild.yaml, om.yaml) "
            "for unreplaced template placeholders and hardcoded sample values before deploying."
        )
    )
    parser.add_argument(
        "manifests",
        nargs="+",
        type=Path,
        help="One or more deployment manifest paths to validate.",
    )
    args = parser.parse_args()

    all_issues: list[str] = []
    for manifest_path in args.manifests:
        try:
            issues = audit_manifest(manifest_path)
            all_issues.extend(issues)
        except (FileNotFoundError, ValueError, OSError) as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2

    if all_issues:
        print("FAIL: Deployment manifest validation found issues:", file=sys.stderr)
        for issue in all_issues:
            print(f"  - {issue}", file=sys.stderr)
        return 1

    checked = ", ".join(str(p) for p in args.manifests)
    print(f"OK: All checked manifests passed validation ({checked}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
