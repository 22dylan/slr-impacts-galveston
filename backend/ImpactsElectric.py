import os, sys
import numpy as np
import pandas as pd
import geopandas as gpd
import json
import networkx as nx

import time
from pyincore import IncoreClient, GeoUtil, Flood

"""
TODO: 
"""

class electricity_access():
    def __init__(self):
        # self.client = IncoreClient()
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_dir = os.path.join(self.file_dir, 'output', "electric")
        self.makedir(self.output_dir)

        self.read_bldg_inv()
        self.read_elec_inv()
        self.read_bldg2elec_df()

    def read_bldg_inv(self):
        path_to_bldg_inv = os.path.join(self.file_dir, "infrastructure", 'bldgs_drs.json')
        G_df = gpd.read_file(path_to_bldg_inv)
        G_df.to_crs(epsg=4269, inplace=True)
        G_df.set_index("guid", inplace=True)
        self.bldg_df = G_df

    def read_elec_inv(self):
        path_to_elec_inv = os.path.join(self.file_dir, "infrastructure", 'substation-galveston.shp')
        G_df = gpd.read_file(path_to_elec_inv)
        G_df.to_crs(epsg=4269, inplace=True)
        G_df.set_index("guid", inplace=True)
        self.elec_df = G_df

    def read_bldg2elec_df(self):
        bldg2elec_df = pd.read_csv(os.path.join(self.file_dir, "infrastructure", "bldg2elec_galveston.csv"))
        bldg2elec_df.set_index("bldg_guid", inplace=True)
        self.bldg2elec_df = bldg2elec_df

    def run_electricity_access(self, slr_ft):
        self.run_slr_exposure(self.elec_df, slr_ft=slr_ft)
        self.run_elec_access(slr_ft=slr_ft)

    def run_elec_access(self, slr_ft):
        path_to_ss_exposure = os.path.join(self.file_dir, "output", "electric", "substation-exposure-{}ft.csv" .format(slr_ft))
        substation_exposure = pd.read_csv(path_to_ss_exposure)
        substation_exposure.set_index("guid", inplace=True)
        df_out = pd.merge(self.bldg2elec_df, substation_exposure["haz_expose"], left_on="node_guid", right_index=True)
        df_out['elec'] = ~df_out['haz_expose']
        df_out['elec'] = df_out['elec']*1
        del df_out['haz_expose']
        
        self.write_out(df_out, slr_ft, "elec-access")



    ###########################################################################
    def run_slr_exposure(self, gdf, slr_ft):
        flood = self.setup_local_hazard(slr_ft)
        hazard_type = "flood"                                                  # Galveston deterministic Hurricane, 3 datasets - Kriging

        hazard_im = []
        haz_expose = []
        cnt = 0
        for ss_i, ss in gdf.iterrows():
            location = GeoUtil.get_location(ss)
            loc = str(location.y) + "," + str(location.x)
            payload = [
                        {
                             "demands": ["inundationDepth"],
                             "units": ["ft"],
                             "loc": loc
                         }
                     ]
            values = flood.read_hazard_values(payload)
            haz_val = values[0]['hazardValues'][0]
            if haz_val < 0:
                hazard_im.append(0)
                haz_expose.append(False)
            else:
                hazard_im.append(haz_val)
                haz_expose.append(True)

            cnt += 1

        gdf_out = pd.DataFrame(index=gdf.index)
        gdf_out['hazard_values'] = hazard_im
        gdf_out['haz_expose'] = haz_expose
        self.write_out(gdf_out, slr_ft, 'substation-exposure')

    def setup_local_hazard(self, slr_ft):
        path_to_data = os.path.join(self.file_dir, "inundation-rasters")

        # create the flood object
        flood = Flood.from_json_file(os.path.join(path_to_data, "slr-dataset.json"))

        # attach dataset from local files
        flood.hazardDatasets[0].from_file((os.path.join(path_to_data, "TX_North2_slr_depth_{}ft.tif" .format(slr_ft))),
                                          data_type="incore:deterministicFloodRaster")

        return flood


    def combine_elec_access(self):
        path_to_elec_accs = os.path.join(self.file_dir,  'output', "electric")
        combined_df = pd.DataFrame()
        
        for slr in range(0,11):
            fname = "elec-access-{}ft.csv" .format(slr)
            fname = os.path.join(path_to_elec_accs, fname)
            df = pd.read_csv(fname)
            df.rename(columns={"bldg_guid":"guid"}, inplace=True)
            df.set_index('guid', inplace=True)
            if slr == 0:
                combined_df.index = df.index
            df = pd.DataFrame(df['elec'])
            rename_dict = {
                            "elec": "elec_{}ft" .format(slr), 
                            }
            df.rename(columns=rename_dict, inplace=True)
            combined_df = pd.merge(combined_df, df, left_index=True, right_index=True)
        

        fn_out = os.path.join(self.file_dir,  'output', "elec-accs-combined.csv")
        combined_df.to_csv(fn_out)

        return combined_df


    ###########################################################################
    def write_out(self, gdf_out, slr_ft, fn):
        f_out = os.path.join(self.output_dir, '{}-{}ft.csv' .format(fn, slr_ft))
        gdf_out.to_csv(f_out)


    def makedir(self, path):
        """ checking if path exists and making it if it doesn't. 
            if the path doesn't exist, make dir and return False (e.g. didn't exist 
                before)
            if the path does exist, return True
        """
        if not os.path.exists(path):
            os.makedirs(path)
            return False
        else:
            return True


if __name__ == "__main__":


    for slr_ft in range(0,11):
        print("----------------------")
        print("SLR: {}" .format(slr_ft))
        ea = electricity_access()
        ea.run_electricity_access(
                        slr_ft=slr_ft,
                        )
        print()


    # # combining results
    ea = electricity_access()
    df = ea.combine_elec_access()
























