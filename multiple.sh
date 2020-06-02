#!/bin/sh
for i in $(seq 1 $1)
do  
    echo '******************Working on Day '$i'********************'
    DAY=$i node mediatailor_quicksight.js
    echo '******************Done with Day '$i'*********************'
done