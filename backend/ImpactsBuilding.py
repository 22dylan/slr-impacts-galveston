import os, sys
import pandas as pd
import json
import geopandas as gpd

from pyincore import IncoreClient, Dataset, FragilityService, DataService, MappingSet, HazardService, FragilityCurveSet, Mapping, Flood
from pyincore.analyses.housingunitallocation import HousingUnitAllocation
from pyincore.analyses.buildingdamage import BuildingDamage


"""
TODO: 
    - figure out missing surgewave archetypes
    - convert slr rasters from feet to meters or foundation height to feet before passing to buildngdamage
    - clean up SLR code (e.g., defining mappings); verify code
    - look into noaa_coop for noaa tide api in python
    
"""

class BuildingExposureSLR():
    def __init__(self):
        self.client = IncoreClient()
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.output_dir = os.path.join(self.file_dir, 'output', "buildings")

        self.makedir(self.output_dir)

    def RunBldgExposure(self, slr_ft):
        bldg_dataset = self.read_bldg_dataset_local()
        self.RunSLRDmg(bldg_dataset, slr_ft)
    
    def read_bldg_dataset_local(self):
        path_to_bldg_dataset = os.path.join(self.file_dir, "infrastructure", 'bldgs_drs.json')
        bldg_dataset = Dataset.from_file(path_to_bldg_dataset, data_type="ergo:buildingInventoryVer7")
        return bldg_dataset

    ###########################################################################
    def RunSLRDmg(self, bldg_ds, slr_ft):
        """ Building content damage from flood.
            Using Nofal's fragility curves and mapping
                https://doi.org/10.3390/w12082277
                Mapping id corresponds to Nofal and van de Lindt (2020) Table 4.
                    - I'm unsure how DS0, DS1, and DS2 are combined to get fragility 
                      curves in IN-CORE. DS3 matches.
        """
        hazard_type = "flood"                                                  # Galveston deterministic Hurricane, 3 datasets - Kriging

        # SLR building content dmg mapping
        mapping_set = self.create_mappingset_slr()

        result_name = os.path.join(self.output_dir, "BldgDmg-SLRContent-{}ft.csv" .format(slr_ft))       # output file name and path

        # pyincore building damage
        bldg_dmg = BuildingDamage(self.client)
        bldg_dmg.set_input_dataset("buildings", bldg_ds)
        bldg_dmg.set_input_dataset("dfr3_mapping_set", mapping_set)
        bldg_dmg.set_parameter("fragility_key", "Non-Retrofit Fragility ID Code")
        bldg_dmg.set_parameter("result_name", result_name)
        bldg_dmg.set_parameter("hazard_type", hazard_type)
        hzrd_dset = self.get_locl_hazard_dset(slr_ft)
        bldg_dmg.set_input_hazard("hazard", hzrd_dset)
    
        bldg_dmg.set_parameter("num_cpu", 8)
        bldg_dmg.run()

    def get_locl_hazard_dset(self, slr):
        path_to_tiff = os.path.join(self.file_dir, "inundation-rasters", "TX_North2_slr_depth_{}ft.tif" .format(slr))
        flood = self.define_dataset_json(slr)
        flood.hazardDatasets[0].from_file(path_to_tiff, data_type="incore:deterministicFloodRaster")
        return flood

    def define_dataset_json(self, SLR_ft):
        slr_dataset_data = {
          "name":"Galveston SLR - {}ft. (MHHW)" .format(SLR_ft),
          "description":"Galveston Sea Level Rise (SLR) - {}ft. (above MHHW); Data from NOAA Digital Coast; SLR relative to MHHW; Map of inundation footprint in feet." .format(SLR_ft),
          "floodType":"dataset",
          "dataType":"incore:deterministicFloodRaster",
          "hazardDatasets":[
            {
                "hazardType": "deterministic",
                "demandType": "inundationDepth",
                "threshold": 0,
                "threshold_unit":"ft",
                "demandUnits": "ft",
                "floodParameters": {
                    "model": "NOAA SLR Inundation"
                }
            }
          ]
        }
        return Flood.from_json_str(json.dumps(slr_dataset_data))

    def create_mappingset_slr(self):
        fragilitysvc = FragilityService(self.client)                           # setting up IN-CORE fragility service
        existing_flood_mapping = self.read_existing_flood_mapping(fragilitysvc) # getting existing flood mapping (for content damage)
        # dfr3_mapping_template = self.read_mapping_template()                   # read template for new mapping
        frag_sets = self.setup_frag_sets(fragilitysvc, existing_flood_mapping) # set up fragility sets
        mapping_set = self.setup_mapping_set(existing_flood_mapping, frag_sets)              # setup mapping sets
        return mapping_set

    def read_existing_flood_mapping(self, fragilitysvc):
        mapping_id = "62fefd688a30d30dac57bbd7"
        mapping_set = fragilitysvc.get_mapping(mapping_id)
        return mapping_set

    def read_mapping_template(self):
        path_to_json = os.path.join(os.getcwd(), 'GalvestonSLRMappingTemplate.json')
        with open(path_to_json) as f:
            d = json.load(f)
        return d

    def setup_frag_sets(self, fragilitysvc, existing_flood_mapping):
        """
        sets up fragilty sets for each archetyp
        returns dictionary of archetyp number (key): fragility set (value)
        """
        frag_sets = {}

        # loop through each mapping in the mapping set
        for mapping in existing_flood_mapping["mappings"]:
            frag_id = mapping["entry"]['Non-Retrofit Fragility ID Code']       # get fragility id
            frag_set = fragilitysvc.get_dfr3_set(dfr3_id=frag_id)              # get dfr3_set associated with fragility id

            """ replacing (surgeLevel-ffe_elev) with (inundationdepth-found_ht) 
                in fragility expression. note that these are the same thing
                as the surgelevel and ffe_elev are both measured from the vertical 
                datum - i.e. this is the freeboard. both inundation depth and 
                foundation height are measured from the ground - i.e. this 
                is also the freeboard.
                
                note: inundationdepth in rasters is measured in feet, whereas 
                foundation height and the fragility curves are in meters. 
                IN-CORE converts the raster layers to the appropriate units.
            """
            for frag_i in range(len(frag_set['fragilityCurves'])):              # loop through fragility curves in fragility set
                frag_set['fragilityCurves'][frag_i]['rules'][0]['condition'] = ["inundationDepth - found_ht > 0"]
                frag_set['fragilityCurves'][frag_i]['rules'][0]['expression'] = frag_set['fragilityCurves'][frag_i]['rules'][0]['expression'].replace("surgeLevel - ffe_elev", "inundationDepth - found_ht")

            arch_num = frag_set['description'].split("archetype")[-1]         # getting archetype number

            del frag_set['id']                  # removing; not needed
            del frag_set['description']         # removing; updating later
            del frag_set['hazardType']          # removing; updating later
            del frag_set['creator']             # removing; not needed
            del frag_set['curveParameters']     # removing; updating later
            del frag_set['spaces']              # removing; not needed
            del frag_set['demandTypes']         # removing; updating later

            frag_set['id'] = arch_num
            frag_set["description"] = "Galveston inundation fragility specific for building archetype{}; from Nofal et al. flood fragility curves adapted for inundationDepth and foundation height above ground" .format(arch_num)
            frag_set['hazardType'] = "flood"
            frag_set["curveParameters"] = [
                    {
                      "name": "inundationDepth",
                      "unit": "m",
                      "description": "water surface elevation above ground",
                      # "fullName": null,
                      # "expression": null
                    },
                    {
                      "name": "found_ht",
                      "unit": "m",
                      "description": "foundation height above ground",
                      # "fullName": null,
                      # "expression": "0.0"
                    }]
            frag_set['demandTypes'] = ["inundationDepth"]
            frag_sets[int(arch_num)] = FragilityCurveSet(frag_set)

        return frag_sets

    def setup_mapping_set(self, existing_flood_mapping, frag_sets):
        """ sets up a mapping set using the existing flood mapping and the fragility
            set provided by the previous functions
            returns pyincore mapping set
        """
        newmapping = existing_flood_mapping.copy()
        del newmapping['id']                # removing; updating later
        del newmapping['name']              # removing; updating later
        del newmapping['hazardType']        # removing; updating later
        del newmapping['mappings']          # removing; updating later
        del newmapping['creator']           # removing; not needed
        del newmapping['spaces']            # removing; not needed

        newmapping['id'] = "temp placeholder"
        newmapping['name'] = "Galveston sea level rise flood fragility mappings for buildings"
        newmapping['hazardType'] = "flood"

        newmapping_list = []
        # for frag_set_i, frag_set in enumerate(frag_sets):
        for arch_num in frag_sets.keys():
            frag_set = frag_sets[arch_num]

            entry = {"Non-Retrofit Fragility ID Code": frag_set}
            rules = [[ "int arch_flood EQUALS {}".format(arch_num)]]
            newmapping_list.append(Mapping(entry, rules))

        newmapping['mappings'] = newmapping_list
        mappingset = MappingSet(newmapping)
        return mappingset

    ###########################################################################
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


"""
used to combine building damage results to one file
"""
class CombineBuildingExpSLR:
    def __init__(self, slr_start=0, slr_end=10, save_df=False):
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.slr_start = slr_start
        self.slr_end = slr_end

        self.bldg_df = self.read_bldg_df()
        self.df = self.combine_bldg_dmg()
        if save_df:
            path_out = os.path.join(self.file_dir, "output", "bldg-exp-combined.csv")

            self.df.to_csv(path_out)

    def combine_bldg_dmg(self):
        path_to_bldg_dmg = os.path.join(self.file_dir,  'output', 'buildings')
        combined_df = pd.DataFrame()
        
        for slr in range(self.slr_start, self.slr_end+1):
            fname = "BldgDmg-SLRContent-{}ft.csv" .format(slr)
            fname = os.path.join(path_to_bldg_dmg, fname)
            df = pd.read_csv(fname)
            df.set_index('guid', inplace=True)
            if slr == 0:
                combined_df.index = df.index

            df = df[['DS_0', 'DS_1', 'DS_2', 'DS_3', 'haz_expose']]

            rename_dict = {
                            "DS_0": "slr{}ft_DS_0" .format(slr), 
                            "DS_1": "slr{}ft_DS_1" .format(slr), 
                            "DS_2": "slr{}ft_DS_2" .format(slr), 
                            "DS_3": "slr{}ft_DS_3" .format(slr),
                            "haz_expose": "slr{}ft_haz_expose" .format(slr)
                            }
            df.rename(columns=rename_dict, inplace=True)
            combined_df = pd.merge(combined_df, df, left_index=True, right_index=True)
        return combined_df



    def read_bldg_df(self):
        path_to_bldgs = os.path.join(self.file_dir, "infrastructure", 'bldgs_drs.json')
        gdf = gpd.read_file(path_to_bldgs)
        gdf.set_index('guid', inplace=True)

        return gdf


    def save_df(self):
        fname = os.path.join(self.file_dir, "BldgDmg-SLR-Combined.csv")
        self.df.to_csv(fname)
        

    def count_n_exposed(self, slr_ft):
        col_name = "slr{}ft_haz_expose" .format(slr_ft)
        exposed = self.df.loc[self.df[col_name]=='yes']
        return len(exposed)


if __name__ == "__main__":
    # for slr_ft in range(0,11):
    #     print("----------------------")
    #     print("SLR: {}" .format(slr_ft))
    #     BD_SLR = BuildingExposureSLR()
    #     BD_SLR.RunBldgDmg(
    #                     recurrence_interval=100,
    #                     slr_ft=slr_ft,
    #                     hurricane_tf=False,
    #                     slr_tf=True,
    #                     locl_bldg_dset=True,
    #                     locl_hzrd=True
    #                     )
    #     print()




    # C = CombineBuildingExpSLR(slr_start=0, slr_end=10, save_df=True)
    # print(C.count_n_exposed(3))

    pass























