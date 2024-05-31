import os, sys
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib as mpl
import datetime

from backend import SLR_Api

class MapWaterLevels:
    def __init__(self, begindate_str, enddate_str, station_id, nonexceendance_probs, destination_points):
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        self.begindate_str = begindate_str
        self.enddate_str = enddate_str
        self.begindate = pd.to_datetime(begindate_str).date()
        self.enddate = pd.to_datetime(enddate_str).date()

        self.bldg_df = self.read_bldg_inv()
        self.bldg_exp_df = self.read_combined_bldg_exp()
        self.elec_acc_df = self.read_combined_elec_acc()
        self.trns_acc_df = {}
        for runname in destination_points:
            self.trns_acc_df[runname] = self.read_combined_trns_acc(runname)
        self.destination_points = destination_points

        self.waterlevels, self.scenarios = self.read_slr_scenarios(station_id, nonexceendance_probs)

        self.nonexceendance_probs = nonexceendance_probs
        self.path_out = os.path.join(self.file_dir, 'output', "impacts-time")
        self.makedir(self.path_out)

    def read_bldg_inv(self):
        path_to_bldg_inv = os.path.join(self.file_dir, "infrastructure", 'bldgs_drs.json')
        G_df = gpd.read_file(path_to_bldg_inv)
        G_df.to_crs(epsg=4269, inplace=True)
        G_df.set_index("guid", inplace=True)
        return G_df

    def read_combined_bldg_exp(self):
        path_to_bldg_dmg = os.path.join(self.file_dir, "output", "bldg-exp-combined.csv")
        df = pd.read_csv(path_to_bldg_dmg, index_col=0)
        return df

    def read_combined_elec_acc(self):
        path_to_elec = os.path.join(self.file_dir, "output", "elec-accs-combined.csv")
        df = pd.read_csv(path_to_elec, index_col=0)
        return df

    def read_combined_trns_acc(self, runname):
        path_to_trns = os.path.join(self.file_dir, "output", "trans-accs-{}-combined.csv" .format(runname))
        df = pd.read_csv(path_to_trns, index_col=0)
        return df

    def read_slr_scenarios(self, station_id, nonexceendance_probs):
        path_to_slr_scenarios = os.path.join(self.file_dir, '..', '')
        SLR = SLR_Api.SLR_API(
            station_id=station_id, 
            scenario_names=['NOAA et al. 2022'], # 'USACE 2013'], # Terri - use NOAA 2022; it's been approved by USACE too
            begin_date=self.begindate_str,
            end_date=self.enddate_str,
            load_tides=True,
            nonexceendance_probs=nonexceendance_probs
            )

        df = SLR.combined_df
        df.index = pd.to_datetime(df.index)
        slr_scenarios = SLR.slr_scenarios
        return df, slr_scenarios


    def map_bldg_impacts(self, scenarios=None, stepsize='days'):
        if stepsize=='days':
            t_steps = self.waterlevels.index.unique()
        elif stepsize == 'years':
            t_steps = self.waterlevels.index.year.unique()

        if scenarios == None:
            source = list(self.scenarios.keys())[0]
            scenarios = self.scenarios[source]

        for ne in self.nonexceendance_probs:
            for scenario_i, scenario in enumerate(scenarios):  # loop through NOAA scenarios (0.3, 0.5, ... 2.0)
                exposed = np.zeros((len(self.bldg_exp_df), len(t_steps)))
                for t_i, t in enumerate(t_steps):      # loop through years
                    scenario_name_w_tide = 'SL+Tide_ft_MHHW_{}_ne{}' .format(scenario, ne)

                    if stepsize == 'days':
                        max_elev = self.waterlevels.loc[self.waterlevels.index==t, scenario_name_w_tide].max()
                    elif stepsize=='years':
                        max_elev = self.waterlevels.loc[self.waterlevels.index.year==t, scenario_name_w_tide].max()

                    slr_layer = self.return_slr_layer([max_elev])
                    exposed[:, t_i] = self.count_exposed(slr_layer, self.bldg_exp_df)
                df_raw = pd.DataFrame(exposed, index=self.bldg_exp_df.index, columns=t_steps)
                
                cols = list(df_raw.columns.astype(str))
                years = self.waterlevels.index.year.unique().to_list()
                ntimes_exposed = np.zeros((len(self.bldg_exp_df), len(years)))
                for year_i, year in enumerate(years):
                    df_cols = [i for i in cols if str(year) in i]
                    df_ = df_raw[df_cols]
                    df_.sum(axis=1)
                    ntimes_exposed[:,year_i] = df_.sum(axis=1)

                df_ntimes_exposed = pd.DataFrame(ntimes_exposed, index=self.bldg_exp_df.index, columns=years)
                scenario_name = self.scenario_to_name(scenario)
                fname = os.path.join(self.path_out, 'nTimesExp_years_sc{}_ne{}.csv' .format(scenario_name, ne))
                df_ntimes_exposed.to_csv(fname)


    def map_elec_impacts(self, scenarios=None, stepsize='days'):
        if stepsize=='days':
            t_steps = self.waterlevels.index.unique()
        elif stepsize == 'years':
            t_steps = self.waterlevels.index.year.unique()

        if scenarios == None:
            source = list(self.scenarios.keys())[0]
            scenarios = self.scenarios[source]

        for ne in self.nonexceendance_probs:
            for scenario_i, scenario in enumerate(scenarios):  # loop through NOAA scenarios (0.3, 0.5, ... 2.0)
                exposed = np.zeros((len(self.elec_acc_df), len(t_steps)))
                for t_i, t in enumerate(t_steps):      # loop through years
                    scenario_name_w_tide = 'SL+Tide_ft_MHHW_{}_ne{}' .format(scenario, ne)

                    if stepsize == 'days':
                        max_elev = self.waterlevels.loc[self.waterlevels.index==t, scenario_name_w_tide].max()
                    elif stepsize=='years':
                        max_elev = self.waterlevels.loc[self.waterlevels.index.year==t, scenario_name_w_tide].max()
                    
                    slr_layer = self.return_slr_layer([max_elev])
                    exposed[:, t_i] = self.count_n_times_no_elec(slr_layer, self.elec_acc_df)

                df_raw = pd.DataFrame(exposed, index=self.elec_acc_df.index, columns=t_steps)          
                cols = list(df_raw.columns.astype(str))
                years = self.waterlevels.index.year.unique().to_list()
                ntimes_exposed = np.zeros((len(self.elec_acc_df), len(years)))
                for year_i, year in enumerate(years):
                    df_cols = [i for i in cols if str(year) in i]
                    df_ = df_raw[df_cols]
                    df_.sum(axis=1)
                    ntimes_exposed[:,year_i] = df_.sum(axis=1)

                df_ntimes_exposed = pd.DataFrame(ntimes_exposed, index=self.elec_acc_df.index, columns=years)
                scenario_name = self.scenario_to_name(scenario)
                fname = os.path.join(self.path_out, 'nNoAccess_years_sc{}_ne{}.csv' .format(scenario_name, ne))
                df_ntimes_exposed.to_csv(fname)
                
    def map_trns_impacts(self, scenarios=None, stepsize='days', threshold=(1/1.25)):
        if stepsize=='days':
            t_steps = self.waterlevels.index.unique()
        elif stepsize == 'years':
            t_steps = self.waterlevels.index.year.unique()

        if scenarios == None:
            source = list(self.scenarios.keys())[0]
            scenarios = self.scenarios[source]

        for runname in self.destination_points:
            for ne in self.nonexceendance_probs:
                for scenario_i, scenario in enumerate(scenarios):  # loop through NOAA scenarios (0.3, 0.5, ... 2.0)
                    exposed = np.zeros((len(self.trns_acc_df[runname]), len(t_steps)))
                    for t_i, t in enumerate(t_steps):      # loop through years
                        scenario_name_w_tide = 'SL+Tide_ft_MHHW_{}_ne{}' .format(scenario, ne)

                        if stepsize == 'days':
                            max_elev = self.waterlevels.loc[self.waterlevels.index==t, scenario_name_w_tide].max()
                        elif stepsize=='years':
                            max_elev = self.waterlevels.loc[self.waterlevels.index.year==t, scenario_name_w_tide].max()

                        slr_layer = self.return_slr_layer([max_elev])
                        exposed[:, t_i] = self.count_n_times_low_access(slr_layer, self.trns_acc_df[runname], threshold)

                    df_raw = pd.DataFrame(exposed, index=self.trns_acc_df[runname].index, columns=t_steps)          
                    cols = list(df_raw.columns.astype(str))
                    years = self.waterlevels.index.year.unique().to_list()
                    ntimes_exposed = np.zeros((len(self.trns_acc_df[runname]), len(years)))
                    for year_i, year in enumerate(years):
                        df_cols = [i for i in cols if str(year) in i]
                        df_ = df_raw[df_cols]
                        df_.sum(axis=1)
                        ntimes_exposed[:,year_i] = df_.sum(axis=1)

                    df_ntimes_exposed = pd.DataFrame(ntimes_exposed, index=self.trns_acc_df[runname].index, columns=years)
                    scenario_name = self.scenario_to_name(scenario)
                    fname = os.path.join(self.path_out, 'nTTIncrease_years_sc{}_ne{}_{}.csv' .format(scenario_name, ne, runname))
                    df_ntimes_exposed.to_csv(fname)

    def count_losses(self, NumDaysExposedBeforeRemoving=367, MaximumElevationsInYearConsider=1, scenarios=None, stepsize='days'):

        t_steps = self.waterlevels.index.year.unique()

        if scenarios == None:
            source = list(self.scenarios.keys())[0]
            scenarios = self.scenarios[source]


        for scenario_i, scenario in enumerate(scenarios):  # loop through NOAA scenarios (0.3, 0.5, ... 2.0)
            losses = np.zeros((len(self.bldg_exp_df), len(t_steps)))
            path_to_exposure = os.path.join(self.path_out, 'nTimesExp_years_sc{}_ne{}.csv' .format(scenario, self.nonexceendance_prob))
            exposure_df = pd.read_csv(path_to_exposure)

            for t_i, t in enumerate(t_steps):      # loop through years
                scenario_name_w_tide = 'SL+Tide_ft_MHHW_{}_ne{}' .format(scenario, self.nonexceendance_prob)
                WL = self.waterlevels.loc[self.waterlevels.index.year==t, scenario_name_w_tide]
                WL_max = WL.nlargest(MaximumElevationsInYearConsider).values
                slr_layers = self.return_slr_layer(WL_max, return_list=True)
                
                for layer in slr_layers:
                    col = "slr{}ft_losses" .format(layer)
                    losses[:,t_i] += self.bldg_exp_df[col]
                remove_bldg_tf = exposure_df[str(t)]>NumDaysExposedBeforeRemoving
                losses[remove_bldg_tf.values, t_i] = self.bldg_df.loc[remove_bldg_tf.values, 'repl_cst']

            df_out = pd.DataFrame(losses, index=self.bldg_exp_df.index, columns=t_steps)
            fname = os.path.join(self.path_out, 'Losses_sc{}_ne{}_n{}_m{}.csv' .format(scenario, self.nonexceendance_prob, NumDaysExposedBeforeRemoving, MaximumElevationsInYearConsider))
            df_out.to_csv(fname)

    def return_slr_layer(self, elev, return_list=False):
        if len(elev)>1:
            slr_layers = []
            for e in elev:
                slr_layers.append(self.return_slr_layer_(e))
            return slr_layers
        else:
            if return_list == True:
                return [self.return_slr_layer_(elev[0])]
            return self.return_slr_layer_(elev[0])


    def return_slr_layer_(self, elev):
        if elev <= 0.5:
            return 0
        elif elev <= 1.5:
            return 1
        elif elev <= 2.5:
            return 2
        elif elev <= 3.5:
            return 3
        elif elev <= 4.5:
            return 4
        elif elev <= 5.5:
            return 5
        elif elev <= 6.5:
            return 6
        elif elev <= 7.5:
            return 7
        elif elev <= 8.5:
            return 8
        elif elev <= 9.5:
            return 9
        else:
            return 10

    def count_exposed(self, slr_ft, df=None):
        col_name = "slr{}ft_haz_expose" .format(slr_ft)
        exposed = (df[col_name]=='yes').astype(int)
        return exposed

    def count_n_times_no_elec(self, slr_ft, df):
        col_name = "elec_{}ft" .format(slr_ft)
        no_elec = (df[col_name]==0).astype(int)
        return no_elec

    def count_n_times_low_access(self, slr_ft, df, threshold):
        col_name = "norm_tt_{}ft" .format(slr_ft)
        exposed = (df[col_name]<threshold).astype(int)
        return exposed


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

    def scenario_to_name(self, scenario_num):
        d = {"0.3":"Low" ,
            "0.5": "IntLow", 
            "1.0": "Int", 
            "1.5": "IntHigh", 
            "2.0": "High", 
        }
        return d[scenario_num]


if __name__ == "__main__":
    C = MapWaterLevels(begindate_str='20250101', 
                              enddate_str='21001231', 
                              station_id=8771450, 
                              nonexceendance_probs=[0.17, 0.5, 0.83])
    

    # NumDaysExposedBeforeRemoving_s = [10, 30, 90, 367]
    # MaximumElevationsInYearConsider_s = [1, 2, 3]
    # for n in NumDaysExposedBeforeRemoving_s:
    #     for m in MaximumElevationsInYearConsider_s:
    #         C.count_losses(NumDaysExposedBeforeRemoving=n, MaximumElevationsInYearConsider=m)



    plt.show()















