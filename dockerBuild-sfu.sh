TAGNAME=$1

show_help()
{
    echo ""
    echo "Usage: ./dockerBuild-sfu.sh tagname"
    echo ""
}

if [[ $# -ne 1 ]] ; then
    show_help
    exit 1
fi

COMMIT=$(git rev-parse --verify HEAD)
docker image build . --force-rm -f ./docker/sfu.Dockerfile -t "vidconf-ion_sfu:latest" -t "vidconf-ion_sfu:${COMMIT}"
docker image tag vidconf-ion_sfu:latest howeli/vidconf-ion_sfu:$TAGNAME
docker push howeli/vidconf-ion_sfu:$TAGNAME
