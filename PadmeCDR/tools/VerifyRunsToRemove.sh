#!/bin/bash

usage() {
    year="$( date +%Y )"
    echo "Usage: $0 [-T data_type] [-D dst_site] [-y year] [-h]" 1>&2
    echo "-T data_type    Define type of data to check (DAQ,MM,TMM)"
    echo "-D dst_site     Define site to verify (LNF,CNAF,CNAF2,KLOE)"
    echo "Default: compare content of year $year on DAQ servers with CNAF" 1>&2
    exit 1
}

# Find where this script is really located: needed to find the corresponding VerifyRun.py script
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
VERIFYRUN=$DIR/VerifyRun.py
if ! [[ -e $VERIFYRUN ]]; then
    echo "ERROR - $VERIFYRUN does not exist"
    usage
fi
if ! [[ -f $VERIFYRUN ]]; then
    echo "ERROR - $VERIFYRUN is not a regular file"
    usage
fi
if ! [[ -x $VERIFYRUN ]]; then
    echo "ERROR - $VERIFYRUN is not executable"
    usage
fi

daq_srv_list=( "l1padme3" "l1padme4" "padmesrv2" )
mm_srv="l0padme2"
tmm_srv="192.168.60.10"
daq_user="daq"
#daq_keyfile="/home/${USER}/.ssh/id_rsa_cdr"
daq_keyfile="${HOME}/.ssh/id_rsa_cdr"
daq_path="/data/DAQ"
data_type="DAQ"
dst_site="CNAF"
year="$( date +%Y )"

while getopts ":T:D:y:h" o; do
    case "${o}" in
        T)
            data_type=${OPTARG}
            ;;
        D)
            dst_site=${OPTARG}
            ;;
        y)
            year=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done

if [[ $dst_site != "CNAF" ]] && [[ $dst_site != "CNAF2" ]] && [[ $dst_site != "LNF" ]] && [[ $dst_site != "KLOE" ]]; then
    echo "ERROR - Destination site can only be CNAF, CNAF2, LNF or KLOE"
    exit 2
fi

# Get list of runs on DAQ servers for given year and verify each of them
if [[ $data_type == "DAQ" ]]; then
    for srv in "${daq_srv_list[@]}"; do
	for run in $( ssh -n -i $daq_keyfile -l $daq_user $srv ls ${daq_path}/${year}/rawdata | grep run_ ); do
	    $VERIFYRUN -R $run -T DAQ -S DAQ -s $srv -D $dst_site
	done
    done
elif [[ $data_type == "MM" ]]; then
    for run in $( ssh -n -i $daq_keyfile -l $daq_user $mm_srv ls ${daq_path}/${year}/mmdata | grep run_ ); do
	$VERIFYRUN -R $run -T MM -S DAQ -s $mm_srv -D $dst_site
    done
elif [[ $data_type == "TMM" ]]; then
    for run in $( ssh -n -i $daq_keyfile -l $daq_user $tmm_srv ls ${daq_path}/${year}/tmmdata | grep run_ ); do
	$VERIFYRUN -R $run -T TMM -S DAQ -s $tmm_srv -D $dst_site
    done
else
    echo "ERROR - Data type only be DAQ, MM or TMM"
    exit 2
fi
