#!/bin/bash
# Create dsarch groups for a dataset
dsid=$1
if [[ -z ${dsid} ]]; then
    echo ${dsid}
    echo "Usage: $0 [dsid]"
    exit
fi
g_file="tmp.$dsid.groups"
dsarch.py -gg -ds $dsid > $g_file

# Remove first and last line
tail -n +2 "$g_file" | head -n -1 > ${g_file}.tmp && mv ${g_file}.tmp ${g_file}

echo "-1<:>catalogs<:>0<:>Intake-ESM catalogs<:><:>P<:>catalogs<:>catalogs<:>" >> ${g_file}
echo "-2<:>Kerchunk Reference Files<:>0<:>Virtual Reference files<:><:>P<:>kerchunk<:>kerchunk<:>" >> ${g_file}

dsarch.py -sg -ds $dsid -if ${g_file} -md

# Cleanup
rm ${g_file}
