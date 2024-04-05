## The structure of the example

The pipeline package is comprised of the `my_package` directory and the `setup.py` file. The package defines the pipeline, the pipeline dependencies, and the input parameters. You can define multiple pipelines in the same package. The `my_package.launcher` module is used to submit the pipeline to a runner.

The `main.py` file provides a top-level entrypoint to trigger the pipeline launcher from a
launch environment.

The `Dockerfile` defines the runtime environment for the pipeline. It also configures the Flex Template, which lets you reuse the runtime image to build the Flex Template.

The `requirements.txt` file defines all Python packages in the dependency chain of the pipeline package. Use it to create reproducible Python environments in the Docker image.

The `metadata.json` file defines Flex Template parameters and their validation rules. It is optional.

## Before you begin

1. Follow the
   [Dataflow setup instructions](../../README.md).

1. [Enable the Cloud Build API](https://console.cloud.google.com/flows/enableapi?apiid=cloudbuild.googleapis.com).

## Create a Cloud Storage bucket

```sh
export PROJECT="smart-integration-beam"
export BUCKET="smart-integration-beam-bucket"
export REGION="us-west1"

gsutil mb -p $PROJECT gs://$BUCKET
```

## Create an Artifact Registry repository

```sh
export REPOSITORY="my-artifact-repo"

gcloud artifacts repositories create $REPOSITORY \
    --repository-format=docker \
    --location=$REGION \
    --project $PROJECT

gcloud auth configure-docker $REGION-docker.pkg.dev
```

## Build a Docker image for the pipeline runtime environment

```sh
export TAG=`date +%Y%m%d-%H%M%S`
export SDK_CONTAINER_IMAGE="$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/qbp_bruteforce_production_integration_image:$TAG"

gcloud builds submit .  --tag $SDK_CONTAINER_IMAGE --project $PROJECT
```

## Build the Flex Template

Using the runtime image as the Flex Template image reduces the number of Docker images that need to be maintained.
It also ensures that the pipeline uses the same dependencies at submission and at runtime.

```sh
export TEMPLATE_FILE=gs://$BUCKET/qbp-bruteforce-production_integration-$TAG.json
export TEMPLATE_IMAGE=$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/qbp-bruteforce-production_integration_image:$TAG

gcloud dataflow flex-template build $TEMPLATE_FILE  \
    --image $SDK_CONTAINER_IMAGE \
    --sdk-language "PYTHON" \
    --metadata-file=metadata.json \
    --project $PROJECT
```

## Run the template

```sh
gcloud dataflow flex-template run "qbp-bruteforce-production-integration-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_FILE \
    --region $REGION \
    --staging-location "gs://$BUCKET/staging" \
    --parameters sdk_container_image=$SDK_CONTAINER_IMAGE \
    --project $PROJECT
```

After the pipeline finishes, use the following command to inspect the output:

```
gsutil cat gs://$BUCKET/output*
```

## Optional: Update the dependencies in the requirements file and rebuild the Docker images

The top-level pipeline dependencies are defined in the `install_requires` section of the `setup.py` file.

The `requirements.txt` file pins all Python dependencies, that must be installed in the Docker container image, including the transitive dependencies. Listing all packages produces reproducible Python environments every time the image is built.
Version control the `requirements.txt` file together with the rest of pipeline code.

When the dependencies of your pipeline change or when you want to use the latest available versions of packages in the pipeline's dependency chain, regenerate the `requirements.txt` file:

```
    python3.11 -m pip install pip-tools   # Use a consistent minor version of Python throughout the project.
    pip-compile ./setup.py
```

If you base your custom container image on the standard Apache Beam base image, to reduce the image size and to give preference to the versions already installed in the Apache Beam base image, use a constraints file:

```
   wget https://raw.githubusercontent.com/apache/beam/release-2.54.0/sdks/python/container/py311/base_image_requirements.txt
   pip-compile --constraint=base_image_requirements.txt ./setup.py
```

Alternatively, take the following steps:

1. Use an empty `requirements.txt` file.
1. Build the SDK container Docker image from the Docker file.
1. Collect the output of `pip freeze` at the last stage of the Docker build.
1. Seed the `requirements.txt` file with that content.

```sh
export PROJECT="smart-integration-beam"
export BUCKET="smart-integration-beam-bucket"
export REGION="us-west1"
export REPOSITORY="my-artifact-repo"

gcloud auth configure-docker $REGION-docker.pkg.dev

export TAG=`date +%Y%m%d-%H%M%S`
export SDK_CONTAINER_IMAGE="$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/qbp_bruteforce_production_integration_image:$TAG"

gcloud builds submit .  --tag $SDK_CONTAINER_IMAGE --project $PROJECT

export TEMPLATE_FILE=gs://$BUCKET/qbp-bruteforce-production_integration-$TAG.json
export TEMPLATE_IMAGE=$REGION-docker.pkg.dev/$PROJECT/$REPOSITORY/qbp-bruteforce-production_integration_image:$TAG

gcloud dataflow flex-template build $TEMPLATE_FILE  \
    --image $SDK_CONTAINER_IMAGE \
    --sdk-language "PYTHON" \
    --metadata-file=metadata.json \
    --project $PROJECT

gcloud dataflow flex-template run "qbp-bruteforce-production-integration-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location $TEMPLATE_FILE \
    --region $REGION \
    --staging-location "gs://$BUCKET/staging" \
    --parameters sdk_container_image=$SDK_CONTAINER_IMAGE \
    --project $PROJECT
```
