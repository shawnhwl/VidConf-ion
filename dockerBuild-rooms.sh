TAGNAME=$(git describe)
if [[ $# -eq 1 ]] ; then
    TAGNAME=$1
fi

./dockerBuild-room.sh ${TAGNAME}
./dockerBuild-room-mgmt.sh ${TAGNAME}
./dockerBuild-room-sentry.sh ${TAGNAME}