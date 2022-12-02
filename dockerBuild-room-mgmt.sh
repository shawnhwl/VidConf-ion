TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

docker image build . --force-rm -f ./docker/app-room-mgmt.Dockerfile -t "vidconf-ion_app-room-mgmt:latest" -t "vidconf-ion_app-room-mgmt:${TAGNAME}"
docker image tag vidconf-ion_app-room-mgmt:latest howeli/vidconf-ion_app-room-mgmt:${TAGNAME}
docker push howeli/vidconf-ion_app-room-mgmt:${TAGNAME}
