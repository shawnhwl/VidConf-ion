TAGNAME=$(git describe)
docker image build . --force-rm -f ./docker/signal.Dockerfile -t "vidconf-ion_signal:latest" -t "vidconf-ion_signal:${TAGNAME}"
docker image tag vidconf-ion_signal:latest howeli/vidconf-ion_signal:${TAGNAME}
docker push howeli/vidconf-ion_signal:${TAGNAME}
