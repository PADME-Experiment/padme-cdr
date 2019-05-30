#!/bin/bash

usage() {
    echo "Usage: $0 -r run [-S src_site] [-D dst_site] [-d dst_dir] [-j jobs] [-h]" 1>&2
    echo "Available source sites: CNAF CNAF2 LNF LNF2"
    echo "Available destination sites: CNAF CNAF2 LNF LNF2 LOCAL"
    echo "Default: copy from CNAF to LOCAL" 1>&2
    exit 1
}

# Find where this script is really located: needed to find the corresponding TransferFile.py script
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
TRANSFERFILE=$DIR/TransferFile.py
if ! [[ -e $TRANSFERFILE ]]; then
    echo "ERROR - $TRANSFERFILE does not exist"
    usage
fi
if ! [[ -f $TRANSFERFILE ]]; then
    echo "ERROR - $TRANSFERFILE is not a regular file"
    usage
fi
if ! [[ -x $TRANSFERFILE ]]; then
    echo "ERROR - $TRANSFERFILE is not executable"
    usage
fi

now() {
    date -u +"%Y-%m-%d %H:%M:%S"
}

# Define Storm access point to CNAF tape library and LNF/LNF2 storage system
srm_cnaf="srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padmeTape"
srm_cnaf2="srm://storm-fe-archive.cr.cnaf.infn.it:8444/srm/managerv2?SFN=/padme"
srm_lnf="srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org"
srm_lnf2="srm://atlasse.lnf.infn.it:8446/srm/managerv2?SFN=/dpm/lnf.infn.it/home/vo.padme.org_scratch"

run=""
year=""
src_site="CNAF"
dst_site="LOCAL"
dst_dir=""
jobs=10
while getopts ":r:y:S:D:d:j:h" o; do
    case "${o}" in
	r)
	    run=${OPTARG}
	    ;;
        S)
            src_site=${OPTARG}
            ;;
        D)
            dst_site=${OPTARG}
            ;;
        d)
            dst_dir=${OPTARG}
            ;;
        j)
            jobs=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done

# Check if run was specified
if [[ -z $run ]]; then
    echo "ERROR - No run specified"
    usage
fi

# Extract year from run name
regex="^run_[0-9]+_([0-9][0-9][0-9][0-9])[0-9]+_[0-9]+$"
if [[ $run =~ $regex ]]; then
    year="${BASH_REMATCH[1]}"
fi
if [[ -z $year ]]; then
    echo "ERROR - Unable to extract year from run name"
    usage
fi

# If destination is LOCAL, check if a directory was specified or set it to the current dir
if [[ $dst_site = "LOCAL" && -z $dst_dir ]]; then
    dst_dir=$( pwd )
fi

# Define full URI of source run directory
if [[ $src_site = "CNAF" ]]; then
    src_run_uri="${srm_cnaf}/daq/${year}/rawdata/${run}"
elif [[ $src_site = "CNAF2" ]]; then
    src_run_uri="${srm_cnaf2}/daq/${year}/rawdata/${run}"
elif [[ $src_site = "LNF" ]]; then
    src_run_uri="${srm_lnf}/daq/${year}/rawdata/${run}"
elif [[ $src_site = "LNF2" ]]; then
    src_run_uri="${srm_lnf2}/daq/${year}/rawdata/${run}"
else
    echo "ERROR - Source site ${src_site} is unknown. Please use CNAF, CNAF2, LNF, or LNF2"
    usage
fi

# Transfer all files from source to destination using parallel tool
echo $( now ) - Copying all files from $src_run_uri using $jobs parallel streams
if [[ $dst_site = "LOCAL" ]]; then
    gfal-ls $src_run_uri | sort | parallel -j $jobs $TRANSFERFILE -F {} -S $src_site -D $dst_site -d $dst_dir -v
else
    gfal-ls $src_run_uri | sort | parallel -j $jobs $TRANSFERFILE -F {} -S $src_site -D $dst_site -v
fi
echo $( now ) - All copy jobs completed
