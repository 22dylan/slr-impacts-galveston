[![DOI](https://zenodo.org/badge/808625708.svg)](https://zenodo.org/doi/10.5281/zenodo.11402964)

This notebook accompanies the manuscript, "Quantifying future local impacts of sea-level rise on buildings and infrastructure".

To run this notebook, complete the following in a terminal:

```
micromamba create -f env.yml
micromamba activate slr_impact_env
pip install noaa_coops
jupyter notebook
```

If you use `conda`, simply replace `micromamba` in the commands above with `conda`.
This will open Jupyter, from which the notebook can be ran.

Note that this repository contains inundation raster files that have been resized from the original inundation rasters used in the analysis. These files were resized due to github's file size limitations.
