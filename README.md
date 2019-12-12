# aws-verifier
Tools for evaluating and preparing content for preservation in AWS

This tool seeks the alignment of the files listed in an "Accession Catalog" with files present in a "Restored Files" database. In the process of alignment, the goal is to classify all of the restored files into one the following five categories:

  1. __perfect matches__: files that are known and unaltered -- can be transferred
  2. __discards__: files that are no longer needed, which can be subdivided into these types:
     1. __deaccessions__: files we no longer need (e.g. Zoomify tiles)
     2. __duplicates__: extra copies of perfect matches
     3. __stowaways__: files that were sent to tape unintentionally (e.g., .DS_Store, thumbs.db, system volume info, etc.) 
  3. __altered files__: files that are known but altered (md5 mismatch)
  4. __missing files__: files that are in the accession catalog but not found in the restored files
  5. __x-files__: 'unexplained' files, or files that are among our restored files but are of unknown origin
  

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
- For remaining assets in *restored files* database (the *x-files*):
   - determine procedure for deaccessioning these files, or
   - determine criteria/procedure to accession these files into our catalog
   
  
  
