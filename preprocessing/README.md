# Preprocessing

Preprocessing of the samples used for TPG studies in batches
This preprocessing:
    - select only cluster gen part projected in HGCAL
    - match clusters to gen part (except for PU)
    - keep only relevant variables


Crab config files are in prod/

1. Edit parameters file with paths, deltar threshold, and number of files per batch
2. Run python3 batch_sample.py -p parameters.py with correct preprocessing file version and ntuples names
3. Merge batches with batches_merge_sample.ipynb
