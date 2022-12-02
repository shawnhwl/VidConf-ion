TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

docker image build . --force-rm -f ./docker/sfu.Dockerfile -t "vidconf-ion_sfu:latest" -t "vidconf-ion_sfu:${TAGNAME}"
docker image tag vidconf-ion_sfu:latest howeli/vidconf-ion_sfu:${TAGNAME}
docker push howeli/vidconf-ion_sfu:${TAGNAME}
