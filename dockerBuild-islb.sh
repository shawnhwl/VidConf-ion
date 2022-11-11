TAGNAME=$(git describe)
docker image build . --force-rm -f ./docker/islb.Dockerfile -t "vidconf-ion_islb:latest" -t "vidconf-ion_islb:${TAGNAME}"
docker image tag vidconf-ion_islb:latest howeli/vidconf-ion_islb:${TAGNAME}
docker push howeli/vidconf-ion_islb:${TAGNAME}
