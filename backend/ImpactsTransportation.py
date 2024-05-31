import os, sys
import pandas as pd
import geopandas as gpd
import json
import geopandas as gpd
import networkx as nx
import numpy as np 

from pyincore import IncoreClient, GeoUtil, Flood

# sys.path.append(os.path.join(os.getcwd(), '..'))
# from misc_funcs import HelperFuncs
# from misc_funcs import create_DFR3_mappings

"""
TODO: 
    
"""

class transportation_exposure():
    def __init__(self):
        self.client = IncoreClient()
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_dir = os.path.join(self.file_dir, 'output', "transportation")

        self.makedir(self.output_dir)

    def run_transportation_exposure(self, slr_ft, locl_hzrd=False):
        gdf, gnx = self.read_trns_dataset_local()
        self.run_slr_exposure(gdf, gnx, slr_ft, locl_hzrd)
    

    def read_trns_dataset_local(self):
        path_to_trns_dataset = os.path.join(self.file_dir, "infrastructure", 'Galveston_Island_Roads_Minor_Bridges_Added.shp')
        # bldg_dataset = Dataset.from_file(path_to_bldg_dataset, data_type="ergo:buildingInventoryVer7")
        gdf = gpd.read_file(path_to_trns_dataset)
        gdf.set_index("guid", inplace=True)
        gdf.to_crs(epsg=4269, inplace=True)    # setting crs for this analysis; flood layers are in EPSG:4269 - NAD83
        gnx = nx.from_pandas_edgelist(gdf, source='from', target='to')
        return gdf, gnx


    ###########################################################################
    def run_slr_exposure(self, gdf, gnx, slr_ft, locl_hzrd):
        if locl_hzrd == True:
            flood = self.setup_local_hazard(slr_ft)
            hazard_type = "flood"                                                  # Galveston deterministic Hurricane, 3 datasets - Kriging

        hazard_im = []
        haz_expose = []
        cnt = 0

        for road_i, road in gdf.iterrows():
            if cnt % 1000 == 0:
                print(cnt)
            
            if road['bridge'] == 'yes':
                hazard_im.append(0)
                haz_expose.append(False)
            else:
                location = GeoUtil.get_location(road)
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

        self.write_out(gdf_out, slr_ft)

    def setup_local_hazard(self, slr_ft):
        path_to_data = os.path.join(self.file_dir, "inundation-rasters")

        # create the flood object
        flood = Flood.from_json_file(os.path.join(path_to_data, "slr-dataset.json"))

        # attach dataset from local files
        flood.hazardDatasets[0].from_file((os.path.join(path_to_data, "TX_North2_slr_depth_{}ft.tif" .format(slr_ft))),
                                          data_type="incore:deterministicFloodRaster")

        return flood


    ###########################################################################
    def write_out(self, gdf_out, slr_ft):
        path_out = os.path.join(self.output_dir, 'transportation-exposure-{}ft.csv' .format(slr_ft))
        gdf_out.to_csv(path_out)


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


class transportation_access():
    def __init__(self):
        # self.client = IncoreClient()
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_dir = os.path.join(self.file_dir, 'output', "transportation")

        self.makedir(self.output_dir)

    def run_transportation_access(self, slr_ft, runname):
        bldg2trns_df, end_nodes = self.read_input_files(runname)
        gdf_ntwk, gnx = self.read_trns_dataset_local(slr_ft)

        df_travel_times = self.run_slr_access(gdf_ntwk, gnx, bldg2trns_df, end_nodes, slr_ft)
        self.write_out(df_travel_times, runname, slr_ft)

    def read_trns_dataset_local(self, slr_ft):
        path_to_trns_dataset = os.path.join(self.file_dir, "infrastructure", 'Galveston_Island_Roads_Minor_Bridges_Added.shp')
        gdf = gpd.read_file(path_to_trns_dataset)
        gdf.set_index("guid", inplace=True)

        gdf.to_crs(epsg=32615, inplace=True)
        del gdf['u']
        del gdf['v']
        del gdf['key']
        del gdf['osmid']
        del gdf['oneway']
        del gdf['ref']
        del gdf['name']
        del gdf['from']
        del gdf['to']
        del gdf['tunnel']
        del gdf['junction']
        del gdf['width']
        del gdf['area']
        del gdf['bridge_inp']
        del gdf['span_mass']
        del gdf['clearance']
        del gdf['g_elev']

        gdf = self.merge_slr_results(gdf, slr_ft)
        gdf = self.assign_speeds(gdf)
        gdf = self.assign_travel_times(gdf)
        gdf.reset_index(inplace=True)

        gnx = nx.from_pandas_edgelist(gdf, 
                                      source='start_node', 
                                      target='end_node', 
                                      edge_key='guid',
                                      edge_attr = ['highway', 'length', 'lanes', 'maxspeed', 'bridge', 'service', 'access', 'travel_time']
                                      )


        gdf.set_index("guid", inplace=True)

        return gdf, gnx



    def combine_trns_access(self, runname):
        path_to_trns_accs = os.path.join(self.file_dir,  'output', "transportation", runname)
        combined_df = pd.DataFrame()
        
        for slr in range(0,11):
            # fname = "BldgDmg-SLRContent-{}ft.csv" .format(slr)
            fname = "travel-times-{}ft.csv" .format(slr)
            fname = os.path.join(path_to_trns_accs, fname)
            df = pd.read_csv(fname)
            df.rename(columns={"bldg_guid":"guid"}, inplace=True)
            df.set_index('guid', inplace=True)
            if slr == 0:
                combined_df.index = df.index
                # combined_df["travel_time_0ft"] = df["travel_time"]
                df["norm_tt"] = 1.0
            else:
                df["norm_tt"] = combined_df["travel_time_0ft"]/df["travel_time"]
            
            df = df[['travel_time', 'norm_tt']]
            rename_dict = {
                            "travel_time": "travel_time_{}ft" .format(slr), 
                            "norm_tt": "norm_tt_{}ft" .format(slr), 
                            }
            df.rename(columns=rename_dict, inplace=True)
            combined_df = pd.merge(combined_df, df, left_index=True, right_index=True)
        
        fn_out = os.path.join(self.file_dir, "output", "trans-accs-{}-combined.csv" .format(runname))
        combined_df.to_csv(fn_out)

        # return combined_df


    def merge_slr_results(self, gdf, slr_ft):
        slr_results = self.read_slr_data(slr_ft)
        gdf = pd.merge(gdf, slr_results, left_index=True, right_index=True)
        return gdf

    def read_slr_data(self, slr_ft):
        path_to_results = os.path.join(self.file_dir, "output", "transportation", 'transportation-exposure-{}ft.csv' .format(slr_ft))
        df = pd.read_csv(path_to_results)
        df.set_index('guid', inplace=True)
        return df

    def assign_speeds(self, gdf):
        gdf['maxspeed'] == None
        gdf.loc[gdf['highway']=='tertiary_link', 'maxspeed'] = 5
        gdf.loc[gdf['highway']=='tertiary', 'maxspeed'] = 35
        gdf.loc[gdf['highway']=='service', 'maxspeed'] = 5
        gdf.loc[gdf['highway']=='secondary_link', 'maxspeed'] = 35
        gdf.loc[gdf['highway']=='secondary', 'maxspeed'] = 35
        gdf.loc[gdf['highway']=='residential', 'maxspeed'] = 25
        gdf.loc[gdf['highway']=='primary_link', 'maxspeed'] = 10
        gdf.loc[gdf['highway']=='primary', 'maxspeed'] = 45
        gdf.loc[gdf['highway']=='pedestrian', 'maxspeed'] = 0
        gdf.loc[gdf['highway']=='motorway_link', 'maxspeed'] = 55
        gdf.loc[gdf['highway']=='motorway', 'maxspeed'] = 60
        gdf.loc[gdf['highway']=='unclassified', 'maxspeed'] = 30

        gdf.loc[gdf['highway']=="['tertiary', 'service']", 'maxspeed'] = 35
        gdf.loc[gdf['highway']=="['tertiary', 'residential']", 'maxspeed'] = 35
        gdf.loc[gdf['highway']=="['service', 'unclassified']", 'maxspeed'] = 5
        gdf.loc[gdf['highway']=="['service', 'residential']", 'maxspeed'] = 5
        gdf.loc[gdf['highway']=="['residential', 'unclassified']", 'maxspeed'] = 25
        gdf.loc[gdf['highway']=="['primary_link', 'unclassified']", 'maxspeed'] = 10

        gdf['maxspeed'] = gdf['maxspeed']*0.44704  # converting mph to m/s;

        gdf = self.flood_speed_relationship(gdf)
        return gdf

    def assign_travel_times(self, gdf):
        gdf['travel_time'] = gdf['length']/gdf['speed']      # getting travel time (distance/speed); length is in meters, speed is in m/s
        gdf['travel_time'] = gdf['travel_time']/60              # converting to minutes
        return gdf

    def flood_speed_relationship(self, gdf):
        """ flood depth to speed relationship provided from Pregnolato et al. (2017)
            https://doi.org/10.1016/j.trd.2017.06.020
            relationship determines max safe speed as a function of flood depth
            units:
                - speed (v) = km/h
                - flood depth (w) = mm
            this function returns the maximum speed along each road segment
        """
        w = gdf['hazard_values']*304.8  # converting flood depth in feet to millimeters for function below

        gdf['maxsafe_speed_flood'] = 0.0009*(w**2) - 0.5529*w + 86.94448
        gdf['maxsafe_speed_flood'] = gdf['maxsafe_speed_flood']/3.6     # converting from km/h to m/s

        gdf.loc[w==0, 'maxsafe_speed_flood'] = gdf.loc[w==0, 'maxspeed'].to_list()
        gdf.loc[w>300, 'maxsafe_speed_flood'] = 0 # can't use 0 because of division, picking really small value instead
        gdf['speed'] = gdf[["maxspeed", "maxsafe_speed_flood"]].min(axis=1)  # taking minimum of maxspeed (speed limit) and maxsafe_speed_flood (flooded driving speed)
        gdf['speed'] = pd.to_numeric(gdf['speed'])
        return gdf


    def read_input_files(self, runname):
        end_nodes = pd.read_csv(os.path.join(self.file_dir, "infrastructure", "{}-end-nodes.csv" .format(runname)))

        bldg2trns_df = pd.read_csv(os.path.join(self.file_dir, "infrastructure", "bldg2trns_galveston.csv"))
        bldg2trns_df.set_index("bldg_guid", inplace=True)

        return bldg2trns_df, end_nodes

    def read_bldg_inv(self):
        path_to_bldg_inv = os.path.join(self.file_dir, "infrastructure", 'bldgs_drs.json')
        G_df = gpd.read_file(path_to_bldg_inv)
        G_df.to_crs(epsg=4269, inplace=True)
        G_df.set_index("guid", inplace=True)
        return G_df

    ###########################################################################
    def run_slr_access(self, gdf_ntwk, gnx, bldg2trns_df, end_nodes, slr_ft):

        sources = bldg2trns_df['node_guid'].unique()
        targets = end_nodes['node'].to_list()

        p_dict = dict(self.path_length_iterator(gnx, sources, targets, weight='travel_time'))
        df_travel_times = pd.DataFrame()
        df_travel_times['source'] = sources.copy()
        targets_save = []
        travel_times = []
        for source in sources:
            travel_time = np.inf
            t_ = ""
            for target in targets:
                travel_time_canidate = p_dict[source][target]
                if travel_time_canidate < travel_time:
                    travel_time = travel_time_canidate
                    t_ = target
            
            travel_times.append(travel_time)
            targets_save.append(t_)

        df_travel_times['target'] = targets_save
        df_travel_times['travel_time'] = travel_times

        bldg2trns_df.reset_index(inplace=True)
        df_out = pd.merge(bldg2trns_df[['bldg_guid', 'node_guid']], df_travel_times, left_on='node_guid', right_on='source', how='left')
        del df_out['node_guid']
        df_out.set_index('bldg_guid', inplace=True)
        return df_out


    def path_length_iterator(self, G, sources, targets, weight):
        """ returns iterator 
            taken from networkx all_pairs_dijkstra_path_length.
            -modified such that it loops through sources as opposed to all nodes
        """
        length = nx.shortest_paths.weighted._dijkstra_multisource
        weight = nx.shortest_paths.weighted._weight_function(G, weight=weight)
        for s in sources:
            yield (s, length(G, sources=[s], weight=weight))


    ###########################################################################
    def write_out(self, gdf_out, runname, slr_ft):
        path_out = os.path.join(self.output_dir, runname)
        self.makedir(path_out)
        f_out = os.path.join(path_out, 'travel-times-{}ft.csv' .format(slr_ft))
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
    for slr_ft in range(1,11):
        print("----------------------")
        print("SLR: {}" .format(slr_ft))
        te = transportation_exposure()
        te.run_transportation_exposure(
                        slr_ft=slr_ft,
                        locl_hzrd=True
                        )
        print()

























