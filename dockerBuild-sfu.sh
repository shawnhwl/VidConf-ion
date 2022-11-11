TAGNAME=$(git describe)
docker image build . --force-rm -f ./docker/sfu.Dockerfile -t "vidconf-ion_sfu:latest" -t "vidconf-ion_sfu:${TAGNAME}"
docker image tag vidconf-ion_sfu:latest howeli/vidconf-ion_sfu:${TAGNAME}
docker push howeli/vidconf-ion_sfu:${TAGNAME}
