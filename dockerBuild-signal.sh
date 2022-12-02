TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

docker image build . --force-rm -f ./docker/signal.Dockerfile -t "vidconf-ion_signal:latest" -t "vidconf-ion_signal:${TAGNAME}"
docker image tag vidconf-ion_signal:latest howeli/vidconf-ion_signal:${TAGNAME}
docker push howeli/vidconf-ion_signal:${TAGNAME}
