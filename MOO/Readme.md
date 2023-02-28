# Notebooks for HGCAL features multi-objective optimization (MOO)

- Alternative-var.ipynb is used to compute the alternative variables used for electromagnetic showers identifications. New studies should use the implementations from CMSSW.
- LUT_estimation.ipynb is used to compute an estiamtion fo the number of ressources (in terms of FPGA LUT) used for BDT operations at a given precision levek and depth.
- BDT_3D_MOO.ipynb performs the MOO with pymoo library using the LUT estiamtion from above. The set of features used here are results from prelimuinary studies
- MOO_results is used to visualize results from the optimization
