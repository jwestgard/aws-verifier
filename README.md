# aws-verifier
Tools for evaluating and preparing content for preservation in AWS

## Verification algorithm (pseudo-code)
- For each asset in *accession catalog*:
   - check for md5 in SQLite lookup table
     - if found, check (md5, filename, bytes) to verfify *perfect match*:
       - add one perfect match to transfer queue
       - add additional perfect matches to *discard* list
     - if no perfect match found:
       - check SQLite for (filename, bytes) matches
       - if found, send file to *altered* list
         - these are files that appear to be corrupt and need evaluation/QC
       - else:
         - add file to *missing* list
- For remaining assets in *restored files* database:
   - determine procedure for deaccessioning these files
  
  
