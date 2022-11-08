TAGNAME=$1

show_help()
{
    echo ""
    echo "Usage: ./dockerBuild-signal.sh tagname"
    echo ""
}

if [[ $# -ne 1 ]] ; then
    show_help
    exit 1
fi

COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/signal.Dockerfile -t "vidconf-ion_signal:latest" -t "vidconf-ion_signal:${COMMIT}"
docker image tag vidconf-ion_signal:latest howeli/vidconf-ion_signal:$TAGNAME
docker push howeli/vidconf-ion_signal:$TAGNAME
