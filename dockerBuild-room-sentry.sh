TAGNAME=$1

show_help()
{
    echo ""
    echo "Usage: ./dockerBuild-room-sentry.sh tagname"
    echo ""
}

if [[ $# -ne 1 ]] ; then
    show_help
    exit 1
fi

COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/app-room-sentry.Dockerfile -t "vidconf-ion_app-room-sentry:latest" -t "vidconf-ion_app-room-sentry:${COMMIT}"
docker image tag vidconf-ion_app-room-sentry:latest howeli/vidconf-ion_app-room-sentry:$TAGNAME
docker push howeli/vidconf-ion_app-room-sentry:$TAGNAME
