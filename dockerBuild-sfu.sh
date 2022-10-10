COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/sfu.Dockerfile -t "vidconf-ion_sfu:latest" -t "vidconf-ion_sfu:${COMMIT}"
docker image tag vidconf-ion_sfu:latest howeli/vidconf-ion_sfu:latest
docker push howeli/vidconf-ion_sfu:latest
