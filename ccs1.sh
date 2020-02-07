#!/usr/bin/env bash

#################################################################################################################################
## configure:

## version stuff:
author='mitch.kostich@jax.org'
version='20200204a'

## self:
cfg_file_dflt='ccs0.cfg'                 ## default configuration file
cmd1=ccs1.sh

task_id="${cmd1/\.sh/}"
task_id="$task_id.$$"
[ -n "$SLURM_ARRAY_JOB_ID" ] && task_id="$task_id.$SLURM_ARRAY_JOB_ID"
[ -n "$SLURM_JOB_ID" ] && task_id="$task_id.$SLURM_JOB_ID"

## environment:
start_time=$(date)
host_name=$(hostname)
ip_address=$(ip route get 1 | awk '{print $NF;exit}')
os_name=$(grep '^NAME=' /etc/os-release)
os_name=${os_name#NAME=}
os_name=${os_name//\"/}
os_version=$(grep '^VERSION=' /etc/os-release)
os_version=${os_version#VERSION=}
os_version=${os_version//\"/}
cpu_number=$(grep -c '^model name' /proc/cpuinfo)
cpu_model=$(grep '^model name' /proc/cpuinfo | head -n1)
cpu_model=${cpu_model#*\: }
mem_total=$(grep 'MemTotal' /proc/meminfo)
mem_total=${mem_total#* }
mem_total=$(echo "$mem_total" | sed 's/^ *//')
slurm_version=$(sbatch --version)
current_dir=$(pwd)

## ccs:
min_min_length='10'
min_min_passes='2'
min_min_rq='0.5'
min_min_snr='1'

#################################################################################################################################
## metadata helper:

exit_meta() {

  local exit_code=$1
  local exit_msg0=$2
  local exit_msg=${exit_msg0//\'/}
  local prog_name=${cmd1/\.sh/}
  local process_id=$$
  local done_time=$(date)

  local arr=("'program_name': '$cmd1'")

  local vars=(author version process_id task_id SLURM_ARRAY_JOB_ID SLURM_JOB_ID start_time done_time exit_code exit_msg \
    host_name ip_address os_name os_version cpu_number cpu_model mem_total)
  local n
  for n in ${vars[*]}; do arr+=("'$n': '${!n}'"); done

  local lnks=(cfg_file in_file idx_file cmd1 cmd2 cmd3 container current_dir out_dir log_dir)
  for n in ${lnks[*]}; do
    val="${!n}"
    [ -n "$val" ] && [ -e "$val" ] && val=$(readlink -f "$val")
    arr+=("'$n': '$val'")
  done

  vars=(cmd2b cmd3b)
  for n in ${vars[*]}; do arr+=("'$n': '${!n}'"); done

  vars=(slurm_version partition qos max_time nodes cpus mem_per_cpu jobid1 jobid2)
  for n in ${vars[*]}; do arr+=("'slurm.$n': '${!n}'"); done

  vars=(min_snr min_length min_passes min_rq)
  for n in ${vars[*]}; do arr+=("'ccs1.$n': '${!n}'"); done

  arr+=("'ccs.report': $ccs_report")

  local pre=$(basename "$cmd2")
  pre=${pre/\.sh/}
  local meta_dir meta_files metadata
  if [ -e "$log_dir" ]; then meta_dir="$log_dir"; else meta_dir="$current_dir"; fi
  if [ -n "$pre" ]; then meta_files=($(ls $meta_dir/${pre}.${jobid1}.*.meta 2>/dev/null)); else meta_files=(); fi
  if [ ${#meta_files[*]} -gt 0 ]; then
    local i
    for((i=0; i<${#meta_files[*]}; ++i)); do
      metadata=$(cat ${meta_files[$i]})
      metadata=$(echo "$metadata" | sed -e 's/ *$//')              ## chomp trailing spaces
      arr+=("'$pre.$i.meta': $metadata")
      rm -f "${meta_files[$i]}"
    done
  else arr+=("'$pre.0.meta': {}")
  fi

  pre=$(basename "$cmd3")
  pre=${pre/\.sh/}
  meta_file=$pre
  [ -n "$meta_file" ] && [ -n "$jobid1" ] && meta_file="$meta_file.$jobid1"
  [ -n "$meta_file" ] && [ -n "$jobid2" ] && meta_file="$meta_file.$jobid2"
  [ -n "$meta_file" ] &&  meta_file="$meta_file.meta"
  if [ -n "$meta_file" ] && [ -e "$log_dir/$meta_file" ]; then meta_file="$log_dir/$meta_file"
  elif [ -n "$meta_file" ] && [ -e "$current_dir/$meta_file" ]; then meta_file="$current_dir/$meta_file"
  fi
  if [ -e "$meta_file" ]; then metadata=$(cat $meta_file); rm -f "$meta_file"; else metadata='{}'; fi
  metadata=$(echo "$metadata" | sed -e 's/ *$//')                  ## get rid of trailing spaces
  arr+=("'$pre.meta': $metadata")

  local o=$(IFS='|'; echo "${arr[*]}")
  if [ -d "$log_dir" ]; then meta_dir="$log_dir"
  else meta_dir="$current_dir"
  fi

  meta_file="$prog_name"
  [ -n "$jobid1" ] && meta_file="$meta_file.$jobid1"
  [ -n "$SLURM_JOB_ID" ] && meta_file="$meta_file.$SLURM_JOB_ID"
  meta_file="$meta_file.$$.meta"

  echo "{${o//\|/, }}" >> $meta_dir/$meta_file
  [ -n "$exit_msg0" ] && echo "$task_id: $exit_msg0" >&2
  exit $exit_code
}

#################################################################################################################################
## load external config file:

load_config() {

  ([ -n "$cfg_file" ] && [ -e "$cfg_file" ] && [ -f "$cfg_file" ] && [ -r "$cfg_file" ]) || 
    exit_meta 100 "Configuration file '$cfg_file' is not a regular file that can be read. $use_msg"

  declare -A keys=(['cmd2']=1 ['cmd3']=1 ['partition']=1 ['qos']=1 ['mem_per_cpu']=1 ['max_time']=1 ['max_nodes']=1 ['max_cpus']=1 \
                   ['nodes_dflt']=1 ['cpus_dflt']=1 ['container']=1 \
                   ['out_dir']=1 ['log_dir']=1 \
                   ['min_snr_dflt']=1 ['min_length_dflt']=1 ['min_passes_dflt']=1 ['min_rq_dflt']=1)

  local line toks key val 
  while read line; do
    [[ "$line" =~ ^\# ]] && continue
    IFS='=' ; read -a toks <<< "$line"; unset IFS
    [ "${#toks[*]}" -lt 2 ] && continue
    key="${toks[0]}"
    unset toks[0]
    val=$(IFS='=' ; echo "${toks[*]}")
    key=$(echo "$key" | tr -d "'" | tr -d '"\\' | sed 's/^ *//; s/ *$//; s/ \+/ /g;')
    val=$(echo "$val" | tr -d "'" | tr -d '"\\' | sed 's/^ *//; s/ *$//; s/ \+/ /g;')
    ( [ -z "$key" ] || [ -z "$val" ] ) && continue
    [[ $key =~ [[:space:]] ]] && continue
    [ -z "${keys[$key]}" ] && continue
    declare -g $key="$val"
    keys[$key]=2
  done < $cfg_file

  for key in ${!keys[*]}; do
    [ "${keys[$key]}" -eq 2 ] || exit_meta 101 "Could not find key '$key' in $cfg_file"
  done
}

#################################################################################################################################
## process/consolidate ccs reports:

process_ccs_reports() {

  local file_pre=$(basename $in_file)
  file_pre="$out_dir/${file_pre/\.bam}.ccs.$jobid1"

  local files f key val i
  if [ "$nodes" -eq 1 ]; then files=($(ls $file_pre.rpt 2>/dev/null)); else files=($(ls $file_pre.*.rpt 2>/dev/null)); fi

  declare -A dat
  local keys=()

  for f in ${files[@]}; do
    while read line; do
      IFS=':' ; read -a toks <<< "$line"; unset IFS
      [ "${#toks[*]}" -lt 2 ] && continue
      key="${toks[0]}"
      unset toks[0]
      val=$(IFS=':' ; echo "${toks[*]}")
      val="${val%%(*}"
      key=$(echo "$key" | tr -d "'" | tr -d '"\\' | sed 's/^ *//; s/ *$//; s/ \+/ /g;')
      val=$(echo "$val" | tr -d "'" | tr -d '"\\' | sed 's/^ *//; s/ *$//; s/ \+/ /g;')
      ( [ -z "$key" ] || [ -z "$val" ] ) && continue
      [ "$val" -eq "$val" ] || continue

      if [ -z "${dat[$key]}" ]; then
        dat[$key]=$val
        keys+=("$key")
      else dat[$key]=$((dat[$key] + val))
      fi
    done < $f
  done

  local report=()
  for ((i=0; i<${#keys[@]}; ++i)); do
    key=${keys[$i]}
    report+=("'$key': '${dat[$key]}'")
  done

  if [ "$nodes" -gt 1 ] && [ ! -e "$file_pre.rpt" ]; then
    for ((i=0; i<${#keys[@]}; ++i)); do
      key=${keys[$i]}
      echo "'$key': '${dat[$key]}'" >> $file_pre.rpt
    done
    rm -f $file_pre.*.rpt
  fi 

  local o=$(IFS='|'; echo "${report[*]}")
  ccs_report="{${o//\|/, }}"
}

#################################################################################################################################
## process arguments:

## usage message:
read -d '' use_msg << EOM
Usage: $cmd1 -i <in_file> [-c <config_file>] [-N <nodes>] [-n <cpus_per_node>] [-s <min_snr>] [-L <min_length>] [-p <min_passes>] [-q <min_rq>]
Parameters:
  <in_file>: name of existing input .bam file with an accompanying .pbi index file (if <nodes> is greater than one); no default (required)
  <config_file>: name of existing text file with configuration information for this run; default: $cfg_file_dflt
  <nodes>: number of compute nodes to utilize; an integer in [1, $max_nodes]; default: $nodes_dflt
  <cpus_per_node>: number of threads to utilize per node; an integer in [1, $max_cpus]; default: $cpus_dflt
  <min_snr>: signal-to-noise ratio for input sub-reads from <in_file>; a number in [$min_min_snr, Inf); default: $min_snr_dflt
  <min_length>: minimum length of output CCS sequence; an integer in [$min_min_length, Inf); default: $min_length_dflt
  <min_passes>: minimum number of passes of template sequence required for CCS processing; an integer in [$min_min_passes, Inf); default: $min_passes_dflt
  <min_rq>: minimum predicted accuracy of output CCS sequence; a number in [$min_min_rq, 1]; default: $min_rq_dflt
EOM

while [[ $# -gt 0 ]]; do
  k="$1"

  case $k in 
  -i)
    in_file="$2"
    shift; shift
    ;;
  -c)
    cfg_file="$2"
    shift; shift
    ;;
  -N)
    nodes="$2"
    shift; shift
    ;;
  -n)
    cpus="$2"
    shift; shift
    ;;
  -s)
    min_snr="$2"
    shift; shift
    ;;
  -L)
    min_length="$2"
    shift; shift
    ;;
  -p)
    min_passes="$2"
    shift; shift
    ;;
  -q)
    min_rq="$2"
    shift; shift
    ;;
  *)
    exit_meta 3 "Error: unrecognized command-line parameter '$k'"
  esac
done
unset k

[ -z "$cfg_file" ] && cfg_file="$cfg_file_dflt"
load_config 

[ -z "$in_file" ] && { exit_meta 4 "Error: $use_msg"; }
[ -z "$nodes" ] && nodes="$nodes_dflt"
[ -z "$cpus" ] && cpus="$cpus_dflt"
[ -z "$min_snr" ] && min_snr="$min_snr_dflt"
[ -z "$min_length" ] && min_length="$min_length_dflt"
[ -z "$min_passes" ] && min_passes="$min_passes_dflt"
[ -z "$min_rq" ] && min_rq="$min_rq_dflt"

## check input file:
([ -e "$in_file" ] && [ -f "$in_file" ] && [ -r "$in_file" ]) || 
  { exit_meta 5 "Error: in_file ($in_file) is not a regular file that can be read."; }
in_file=$(readlink -f "$in_file")

## check numeric parameters:
[ "$nodes" -eq "$nodes" 2>/dev/null ] && [ "$nodes" -ge 1 ] || { exit_meta 8 "Error: nodes must be an integer >= 1"; }
[ "$nodes" -gt "$max_nodes" 2>/dev/null ] && { exit_meta 9 "Error: nodes ($nodes) exceeds max allowed ($max_nodes)"; }
[ "$cpus" -eq "$cpus" 2>/dev/null ] && [ "$cpus" -ge 1 ] || { exit_meta 10 "Error: cpus_per_node must be an integer >= 1"; }
[ "$cpus" -gt "$max_cpus" ] && { exit_meta 11 "Error: cpus_per_node ($cpus) exceeds max allowed ($max_cpus)"; }
[[ "$min_snr" =~ ^[+-]?[0-9]+\.?$ || "$min_snr" =~ ^[+-]?[0-9]*\.[0-9]+$ ]] || 
  { exit_meta 12 "Error: min_snr should be a number >= $min_min_snr"; }
if (( $(echo "$min_snr < $min_min_snr" | bc -l) )); then
  exit_meta 13 "Error: min_snr should be a number >= $min_min_snr"; 
fi
[ "$min_length" -eq "$min_length" 2>/dev/null ] && [ "$min_length" -ge "$min_min_length" ] || \
  { exit_meta 14 "Error: min_length should be an integer >= $min_min_length"; }
[ "$min_passes" -eq "$min_passes" 2>/dev/null ] && [ "$min_passes" -ge "$min_min_passes" ] || \
  { exit_meta 15 "Error: min_passes should be an integer >= $min_min_passes"; }
[[ "$min_rq" =~ ^[+-]?[0-9]+\.?$ || "$min_rq" =~ ^[+-]?[0-9]*\.[0-9]+$ ]] ||
  { exit_meta 16 "Error: min_rq should be a number >= $min_min_rq"; }
if (( $(echo "$min_rq < $min_min_rq" | bc -l) )); then
  exit_meta 17 "Error: min_rq should be a number >= $min_min_rq"
fi

## check index file:

if [ "$nodes" -gt 1 ]; then
  idx_file=$in_file.pbi
  ([ -e "$idx_file" ] && [ -f "$idx_file" ] && [ -r "$idx_file" ]) || 
    { exit_meta 6 "Error: index file '$idx_file' is not a regular file that can be read."; }
fi

## check cmd:
hash "$cmd2" 2>/dev/null || { exit_meta 18 "Error: command ($cmd2) not in PATH ($PATH)."; }
if [ "$nodes" -gt 1 ]; then
  hash "$cmd3" 2>/dev/null || { exit_meta 19 "Error: command ($cmd3) not in PATH ($PATH)."; }
fi

################################################################################################################################
## check and set up (if needed) output directories:

out_dir=$(readlink -f "$out_dir")
mkdir -p "$out_dir"
[ -e "$out_dir" ] && [ -d "$out_dir" ] && [ -w "$out_dir" ] || \
  { exit_meta 20 "Error: failed to create out_dir ($out_dir)."; }

log_dir=$(readlink -f "$log_dir")
mkdir -p "$log_dir"
[ -e "$out_dir" ] && [ -d "$log_dir" ] && [ -w "$out_dir" ] || \
  { exit_meta 21 "Error: failed to create log_dir ($log_dir)."; }

################################################################################################################################
## run job:

params=("$task_id" "$(hostname)" "$cmd1" "$partition" "$nodes" "$cpus" "$mem_per_cpu" "$max_time" \
        "$out_dir" "$log_dir" "$(readlink -f $cmd2)" "$(readlink -f $in_file)" "$min_snr" "$min_length" "$min_passes" "$min_rq")

echo $(IFS='|' ; echo "${params[*]}") >> $log_dir/${cmd1/\.sh}.prm
unset params

if [ "$nodes" -eq 1 ]; then

  ## singleton job:

  cmd2b="sbatch -W -D . -p $partition -c $cpus -t $max_time -q $qos --export ALL --acctg-freq 5 --mail-type NONE \
    --mem-per-cpu $mem_per_cpu $cmd2 $cpus $in_file $container $out_dir $log_dir $min_snr $min_length $min_passes $min_rq"

  tm1=$(date +%s)
  jobid1=$($cmd2b)
  ecode=$?
  tm2=$(date +%s)
  jobid1=${jobid1##* }              ## only keep last word, which should be jobid
  echo "$task_id|$cmd2|$nodes|$cpus|$jobid1|$tm1|$tm2|$ecode|$cmd2b" >> $log_dir/${cmd1/\.sh}.log
  [ "$ecode" -ne 0 ] && { exit_meta 22 "Error: $cmd2 ($cmd2b) failed with exit code '$ecode'."; }
  unset tm1 ecode tm2
else

  ## array job:

  cmd2b="sbatch -W -D . -a 0-$((nodes - 1)) -p $partition -c $cpus -t $max_time -q $qos --export ALL --acctg-freq 5 --mail-type NONE \
    --mem-per-cpu $mem_per_cpu $cmd2 $cpus $in_file $container $out_dir $log_dir $min_snr $min_length $min_passes $min_rq"

  tm1=$(date +%s)
  jobid1=$($cmd2b)
  ecode=$?
  tm2=$(date +%s)
  jobid1=${jobid1##* }              ## only keep last word, which should be jobid
  echo "$task_id|$cmd2|$nodes|$cpus|$jobid1|$tm1|$tm2|$ecode|$cmd2b" >> $log_dir/${cmd1/\.sh}.log
  [ "$ecode" -ne 0 ] && { exit_meta 23 "Error: $cmd2 ($cmd2b) failed with exit code '$ecode'."; }

  ## consolidate and index array outputs

  cmd3b="sbatch -W -D . -p $partition -c $cpus -t $max_time -q $qos --export ALL --acctg-freq 5 --mail-type NONE \
    --mem-per-cpu $mem_per_cpu $cmd3 $cpus $in_file $jobid1 $container $out_dir $log_dir"

  tm1=$(date +%s)
  jobid2=$($cmd3b)
  ecode=$?
  tm2=$(date +%s)
  jobid2=${jobid2##* }              ## only keep last word, which should be jobid
  echo "$task_id|$cmd3|$nodes|$cpus|$jobid2|$tm1|$tm2|$ecode|$cmd3b" >> $log_dir/${cmd1/\.sh}.log
  [ "$ecode" -ne 0 ] && { exit_meta 24 "Error: $cmd3 ($cmd3b) failed with exit code '$ecode'."; }

  unset tm1 ecode tm2
fi

process_ccs_reports

## resource usage:
 
out=${task_id}$'\t'$(sacct -j $jobid1 -P -n --delimiter $'\t' --units M --format ALL)
echo "$out" >> $log_dir/${cmd1/\.sh}.act

if [ "$nodes" -gt 1 ]; then
  out=${task_id}$'\t'$(sacct -j $jobid2 -P -n --delimiter $'\t' --units M --format ALL)
  echo "$out" >> $log_dir/${cmd1/\.sh}.act
fi
unset out

exit_meta 0 'Completed normally'

## DONE #########################################################################################################################

