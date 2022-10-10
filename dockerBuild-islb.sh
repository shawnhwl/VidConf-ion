COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/islb.Dockerfile -t "vidconf-ion_islb:latest" -t "vidconf-ion_islb:${COMMIT}"
docker image tag vidconf-ion_islb:latest howeli/vidconf-ion_islb:latest
docker push howeli/vidconf-ion_islb:latest
