#!/usr/bin/env bash
# Build the PyPOTS imputation service image: sparkworks/pypots:dev by default.
#
#   ./build.sh            build the image
#   ./build.sh --push     the same, then push the tag to Docker Hub (needs `docker login` as a member of sparkworks)
#
#   IMAGE=sparkworks/pypots:dev         the tag to build and push
#   WSP_REPO=../WaveStitchPlus          where the WaveStitchPlus checkout is (it holds the PyPOTS runner)
set -euo pipefail
cd "$(dirname "$0")"

IMAGE="${IMAGE:-sparkworks/pypots:dev}"
WSP_REPO="${WSP_REPO:-../WaveStitchPlus}"

mkdir -p runner
trap 'rm -rf runner' EXIT
cp "$WSP_REPO/dockers/tools/PyPOTS_app/run_imputation.py" "$WSP_REPO/dockers/tools/PyPOTS_app/requirements.txt" runner/

docker build -t "$IMAGE" .
echo "Built $IMAGE"

if [[ "${1:-}" == "--push" ]]; then
    docker push "$IMAGE"
    echo "Pushed $IMAGE"
fi
