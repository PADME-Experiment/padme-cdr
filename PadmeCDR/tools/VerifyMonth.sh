#!/bin/bash

# Prepare a variable with usage guidelines
read -r -d '' usage <<EOF
Usage: $0 -m month [-S site] [-D site] [-j jobs] [-v] [-h]
-m month     Define month to verify in the format yyyymm (e.g. 202010 for october 2020)
-T data_type Define type of data to check (DAQ,MM,TMM)"
-S site      Define comparison source site. Available sites: CNAF CNAF2 LNF LNF2
-D site      Define comparison destination site. Available sites: CNAF CNAF2 LNF LNF2 KLOE
-v           Enable verbose mode (i.e. show list of missing files)
-h           Show this help message and exit
Default comparison: verify CNAF vs LNF
EOF

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

# Define Storm access point to CNAF tape library and LNF/LNF2 storage systems
srm_cnaf="davs://xfer-archive.cr.cnaf.infn.it:8443/padmeTape"
srm_cnaf2="davs://xfer-archive.cr.cnaf.infn.it:8443/padme"
srm_lnf="root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org"
srm_lnf2="root://atlasse.lnf.infn.it//dpm/lnf.infn.it/home/vo.padme.org_scratch"
data_type="DAQ"
src_site="CNAF"
dst_site="LNF"
month=""
jobs=20
verbose=""
while getopts ":m:S:D:j:vh" o; do
    case "${o}" in
        m)
            month=${OPTARG}
            ;;
        T)
            data_type=${OPTARG}
            ;;
        S)
            src_site=${OPTARG}
            ;;
        D)
            dst_site=${OPTARG}
            ;;
        j)
            jobs=${OPTARG}
            ;;
        v)
            verbose="-v"
            ;;
        h)
            echo "$usage"
	    exit 0
            ;;
        *)
	    echo "$usage" 1>&2
	    exit 1
            ;;
    esac
done

if [[ -z $month ]]; then
    echo "ERROR - Please specify month" 1>&2
    echo "$usage" 1>&2
    exit 1
fi
year=${month:0:4}

# Define full URI of source run directory
if [[ $src_site = "CNAF" ]]; then
    src_uri=$srm_cnaf
elif [[ $src_site = "CNAF2" ]]; then
    src_uri=$srm_cnaf2
elif [[ $src_site = "LNF" ]]; then
    src_uri=$srm_lnf
elif [[ $src_site = "LNF2" ]]; then
    src_uri=$srm_lnf2
else
    echo "ERROR - Source site ${src_site} is unknown. Please use CNAF, CNAF2, LNF, or LNF2" 1>&2
    echo "$usage" 1>&2
    exit 1
fi

if [[ $dst_site != "CNAF" ]] && [[ $dst_site != "CNAF2" ]] && [[ $dst_site != "LNF" ]] && [[ $dst_site != "LNF2" ]] && [[ $dst_site != "KLOE" ]]; then
    echo "ERROR - Destination site ${dst_site} is unknown. Please use CNAF, LNF, LNF2, or KLOE" 1>&2
    echo "$usage" 1>&2
    exit 1
fi

if [[ $src_site = $dst_site ]]; then
    echo "ERROR - Source and destination sites are the same: ${src_site}." 1>&2
    echo "$usage" 1>&2
    exit 1
fi

if [[ $data_type == "DAQ" ]]; then
    data_dir="rawdata"
elif [[ $data_type == "MM" ]]; then
    data_dir="mmdata"
elif [[ $data_type == "TMM" ]]; then
    data_dir="tmmdata"
else
    echo "ERROR - Data type only be DAQ, MM or TMM"
    exit 2
fi

run_list=()
for run in $(gfal-ls $src_uri/daq/$year/$data_dir | grep _${month}[0-9][0-9]_ | sort)
do
    run_list+=("$run")
done
if [ ${#run_list[@]} -eq 0 ]; then
    echo "WARNING - No runs found on source site ${src_site} for month ${month}."
else
    if [[ $dst_site != "KLOE" ]]; then
	parallel $VERIFYRUN -R {} -T $data_type -S $src_site -D $dst_site $verbose ::: "${run_list[@]}"
    else
	# KLOE site has problems with multiple ssh accesses: do not use parallel
	for run in "${run_list[@]}"
	do
	    $VERIFYRUN -R $run -S $src_site -D $dst_site $verbose
	done
    fi
fi
