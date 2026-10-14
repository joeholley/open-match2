# OpenTelemetry Collector Sidecar

Open Match 2 uses the [Google-Built OpenTelemetry Collector](https://cloud.google.com/stackdriver/docs/instrumentation/opentelemetry-collector-google-built) Docker image (`us-docker.pkg.dev/cloud-ops-agents-artifacts/google-cloud-opentelemetry-collector/otelcol-google`, or alternatively `otel/opentelemetry-collector-contrib`), run as a sidecar container to collect and export OTLP metrics to Google Managed Prometheus. The `Dockerfile` copies `config.yaml` to both `/etc/otelcol-google/config.yaml` (used by `otelcol-google`) and `/etc/otelcol-contrib/config.yaml` (used by `otel/opentelemetry-collector-contrib`). The example collector configuration is based on the [Google Cloud Go samples repository for Cloud Run](https://github.com/GoogleCloudPlatform/golang-samples/tree/main/run/custom-metrics/collector). You can find more details in the [Write OTLP metrics by using an OpenTelemetry sidecar](https://cloud.google.com/run/docs/tutorials/custom-metrics-opentelemetry-sidecar) documentation.

## Open Match Users
The Open Match artifact registry maintains a pre-built OpenTelemetry Collector image with the necessary config file included, and it's already referenced directly in the `deploy/cloud_run/service.yaml` and `deploy/cloud_run/cloudbuild.yaml` examples. You shouldn't need to modify or rebuild the image unless you're doing something advanced, like replatforming Open Match or replacing the OpenTelemetry backend with a different metrics/tracing/logging provider.

## Open Match Maintainers
To build the OpenTelemetry Collector sidecar image:
1.  Make sure your `gcloud` installation is configured to use the Open Match development project
1.  Make sure your `gcloud` installation has the `artifacts/location` config set to the location for the Open Match artifact registry
1.  Run the following command from this directory:

 `gcloud builds submit . --tag $(gcloud config get artifacts/location)-docker.pkg.dev/$(gcloud config get project)/open-match/otel-collector-sidecar`

