TAGNAME=$1

show_help()
{
    echo ""
    echo "Usage: ./dockerBuild-room.sh tagname"
    echo ""
}

if [[ $# -ne 1 ]] ; then
    show_help
    exit 1
fi

COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/app-room.Dockerfile -t "vidconf-ion_app-room:latest" -t "vidconf-ion_app-room:${COMMIT}"
docker image tag vidconf-ion_app-room:latest howeli/vidconf-ion_app-room:$TAGNAME
docker push howeli/vidconf-ion_app-room:$TAGNAME
