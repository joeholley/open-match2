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
    "OM_CACHE_OUT_MAX_PACKED_KEYS",
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

    has_otelcol_google_base = False
    has_otelcol_contrib_base = False
    has_otelcol_google_config = False
    has_otelcol_contrib_config = False
    entrypoint_line: tuple[int, str] | None = None
    has_cmd_or_config_flag = False

    is_knative_service = False
    is_om_core_service = False
    has_cpu_throttling_false = False
    has_gen2_exec_env = False
    min_scale_value: int | None = None

    is_cloudbuild_run_deploy = False
    has_no_cpu_throttling_flag = False
    has_exec_env_gen2_flag = False
    has_min_instances_flag = False

    for line_num, raw_line in enumerate(lines, start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if re.match(r"^apiVersion:\s*serving\.knative\.dev/v1\b", stripped):
            is_knative_service = True
        if re.match(r"^name:\s*['\"]?om-core['\"]?\b", stripped) or "_JOB_NAME: om-core" in stripped:
            is_om_core_service = True
        if re.search(r"run\.googleapis\.com/cpu-throttling:\s*['\"]?false['\"]?", stripped, re.IGNORECASE):
            has_cpu_throttling_false = True
        if re.search(r"run\.googleapis\.com/execution-environment:\s*['\"]?gen2['\"]?", stripped, re.IGNORECASE):
            has_gen2_exec_env = True
        min_scale_match = re.search(r"autoscaling\.knative\.dev/minScale:\s*['\"]?(\d+)['\"]?", stripped)
        if min_scale_match:
            min_scale_value = int(min_scale_match.group(1))

        if stripped in ("- deploy", "deploy"):
            is_cloudbuild_run_deploy = True
        if "--no-cpu-throttling" in stripped:
            has_no_cpu_throttling_flag = True
        if "--execution-environment" in stripped:
            has_exec_env_gen2_flag = True
        if "--min-instances" in stripped:
            has_min_instances_flag = True

        if re.match(r"^FROM\b", stripped, re.IGNORECASE):
            if "otelcol-google" in stripped:
                has_otelcol_google_base = True
            if "opentelemetry-collector-contrib" in stripped:
                has_otelcol_contrib_base = True

        if re.match(r"^(ADD|COPY)\b", stripped, re.IGNORECASE):
            if re.search(r"\bconfig\.yaml\s+/etc/otelcol-google/config\.yaml\b", stripped):
                has_otelcol_google_config = True
            if re.search(r"\bconfig\.yaml\s+/etc/otelcol-contrib/config\.yaml\b", stripped):
                has_otelcol_contrib_config = True

        if re.match(r"^ENTRYPOINT\b", stripped, re.IGNORECASE):
            entrypoint_line = (line_num, stripped)
            if "--config" in stripped:
                has_cmd_or_config_flag = True

        if re.match(r"^CMD\b", stripped, re.IGNORECASE):
            has_cmd_or_config_flag = True

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

    if has_otelcol_google_base and not has_otelcol_google_config:
        issues.append(
            f"{file_path}: Dockerfile uses 'otelcol-google' base image but does not ADD/COPY config.yaml to /etc/otelcol-google/config.yaml"
        )
    if has_otelcol_contrib_base and not has_otelcol_contrib_config:
        issues.append(
            f"{file_path}: Dockerfile uses 'opentelemetry-collector-contrib' base image but does not ADD/COPY config.yaml to /etc/otelcol-contrib/config.yaml"
        )
    if (has_otelcol_google_base or has_otelcol_contrib_base) and entrypoint_line and not has_cmd_or_config_flag:
        ep_line_num, ep_text = entrypoint_line
        issues.append(
            f"{file_path}:{ep_line_num}: Overriding ENTRYPOINT without CMD or --config clears the base collector image's default --config CMD: {ep_text}"
        )

    if is_knative_service and is_om_core_service:
        if not has_cpu_throttling_false:
            issues.append(
                f"{file_path}: Cloud Run om-core Service must set run.googleapis.com/cpu-throttling: 'false' so background replication goroutines are not throttled between requests"
            )
        if not has_gen2_exec_env:
            issues.append(
                f"{file_path}: Cloud Run om-core Service should set run.googleapis.com/execution-environment: gen2"
            )
        if min_scale_value is None or min_scale_value < 1:
            issues.append(
                f"{file_path}: Cloud Run om-core Service must set autoscaling.knative.dev/minScale >= 1 (got {min_scale_value}) to keep warm instances with replicated ticket caches"
            )

    if is_cloudbuild_run_deploy and is_om_core_service:
        if not has_no_cpu_throttling_flag:
            issues.append(
                f"{file_path}: Cloud Build gcloud run deploy for om-core must include --no-cpu-throttling"
            )
        if not has_exec_env_gen2_flag:
            issues.append(
                f"{file_path}: Cloud Build gcloud run deploy for om-core should include --execution-environment gen2"
            )
        if not has_min_instances_flag:
            issues.append(
                f"{file_path}: Cloud Build gcloud run deploy for om-core must include --min-instances"
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
