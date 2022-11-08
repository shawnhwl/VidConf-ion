TAGNAME=$1

show_help()
{
    echo ""
    echo "Usage: ./dockerBuild-islb.sh tagname"
    echo ""
}

if [[ $# -ne 1 ]] ; then
    show_help
    exit 1
fi

COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/islb.Dockerfile -t "vidconf-ion_islb:latest" -t "vidconf-ion_islb:${COMMIT}"
docker image tag vidconf-ion_islb:latest howeli/vidconf-ion_islb:$TAGNAME
docker push howeli/vidconf-ion_islb:$TAGNAME
