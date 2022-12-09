TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

./dockerBuild-room-mgmt.sh ${TAGNAME}
./dockerBuild-room-sentry.sh ${TAGNAME}
./dockerBuild-room-recorder.sh ${TAGNAME}
./dockerBuild-room-playback.sh ${TAGNAME}