TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

docker image build . --force-rm -f ./docker/app-room-playback.Dockerfile -t "vidconf-ion_app-room-playback:latest" -t "vidconf-ion_app-room-playback:${TAGNAME}"
docker image tag vidconf-ion_app-room-playback:latest howeli/vidconf-ion_app-room-playback:${TAGNAME}
docker push howeli/vidconf-ion_app-room-playback:${TAGNAME}
