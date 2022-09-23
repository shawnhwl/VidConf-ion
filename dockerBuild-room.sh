COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/app-room.Dockerfile -t "vidconf-ion_app-room:latest" -t "vidconf-ion_app-room:${COMMIT}"
docker image tag vidconf-ion_app-room:latest howeli/vidconf-ion_app-room:latest
docker push howeli/vidconf-ion_app-room:latest
