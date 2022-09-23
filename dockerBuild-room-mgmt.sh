COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/app-room-mgmt.Dockerfile -t "vidconf-ion_app-room-mgmt:latest" -t "vidconf-ion_app-room-mgmt:${COMMIT}"
docker image tag vidconf-ion_app-room-mgmt:latest howeli/vidconf-ion_app-room-mgmt:latest
docker push howeli/vidconf-ion_app-room-mgmt:latest
