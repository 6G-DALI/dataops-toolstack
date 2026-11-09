#!/usr/bin/env bash
# Build the WaveStitch+ imputation service image: sparkworks/wavestitch:dev by default.
#
#   ./build.sh            build wavestitchplus-cpu:latest from the WaveStitchPlus repo, then the service on top of it
#   ./build.sh --push     the same, then push the tag to Docker Hub (needs `docker login` as a member of sparkworks)
#
#   IMAGE=sparkworks/wavestitch:dev     the tag to build and push
#   WSP_REPO=../WaveStitchPlus          where the WaveStitchPlus checkout is
#   SKIP_BASE=1                         reuse the wavestitchplus-cpu:latest already built
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-sparkworks/wavestitch:dev}"
WSP_REPO="${WSP_REPO:-../WaveStitchPlus}"

if [[ -z "${SKIP_BASE:-}" ]]; then
    # The project's own script: it vendors the preprocess helper into the build context, builds, and cleans up.
    (cd "$WSP_REPO/dockers/tools" && bash build_image_cpu.sh)
fi

docker build -t "$IMAGE" --build-arg BASE=wavestitchplus-cpu:latest .
echo "Built $IMAGE"

if [[ "${1:-}" == "--push" ]]; then
    docker push "$IMAGE"
    echo "Pushed $IMAGE"
fi
