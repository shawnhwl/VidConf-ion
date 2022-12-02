TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

docker image build . --force-rm -f ./docker/app-room-recorder.Dockerfile -t "vidconf-ion_app-room-recorder:latest" -t "vidconf-ion_app-room-recorder:${TAGNAME}"
docker image tag vidconf-ion_app-room-recorder:latest howeli/vidconf-ion_app-room-recorder:${TAGNAME}
docker push howeli/vidconf-ion_app-room-recorder:${TAGNAME}
