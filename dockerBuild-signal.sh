COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/signal.Dockerfile -t "vidconf-ion_signal:latest" -t "vidconf-ion_signal:${COMMIT}"
docker image tag vidconf-ion_signal:latest howeli/vidconf-ion_signal:latest
docker push howeli/vidconf-ion_signal:latest
