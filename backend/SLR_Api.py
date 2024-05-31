import os, sys
import pandas as pd
import numpy as np
import datetime

from noaa_coops import Station
import matplotlib.pyplot as plt
import matplotlib as mpl



"""
Stations:
    Galveston Pier 21: 8771450 (back bay - downtown)
    Galveston Pleasure Pier: 8771510 (coastal)
    Jamaica Beach: 8771722 (back bay - west end)
"""

class SLR_API():
    def __init__(self, station_id=8771450, scenario_names=None, begin_date="20230101", end_date="20431231", nonexceendance_probs=[0.5], load_tides=True):
        self.file_dir = os.path.dirname(os.path.realpath(__file__))

        slr_df = self.read_slr_data(station_id=station_id)
        slr_scenarios = self.define_slr_scenarios(scenario_names)

        datums = self.define_datums()
        if load_tides:
            tide_df = self.read_tide_data(station_id=station_id, begin_date=begin_date, end_date=end_date)
            self.combined_df = self.combine_tide_slr(slr_df, slr_scenarios, tide_df, datums, nonexceendance_probs)

        self.scenario_names = scenario_names
        self.slr_scenarios = slr_scenarios
        self.station_id = station_id
        self.slr_df = slr_df
    
    def read_slr_data(self, station_id):
        files_in_dir = os.listdir(os.path.join(self.file_dir, 'water-level-data'))

        station_files = [i for i in files_in_dir if str(station_id) in i]
        station_files = [i for i in station_files if "SLT" in i]
        slr_file = [i for i in station_files if ".csv" in i][0]
        df = pd.read_csv(os.path.join(self.file_dir, 'water-level-data', slr_file))
        df['Month'].fillna(1, inplace=True)
        df['Day'] = 1
        df['datetime'] = pd.to_datetime(df[['Year', "Month", "Day"]])
        df.index = df['datetime']
        df = df[['Source', 'Scenario', 'Value Type', 'Sea Level (feet)', 'Nonexceedence Probability']]
        return df

    def read_tide_data(self, station_id, begin_date, end_date):
        fname = "NOAA_Tide_{}_MHHW_{}-{}.csv" .format(station_id, begin_date, end_date)
        if fname not in os.listdir(os.path.join(self.file_dir, 'water-level-data')):
            na = NOAA_API(station_id=station_id, begin_date=begin_date, end_date=end_date)
            na.save_to_csv()
            tide_df = na.df
            tide_df['datum'] = na.datum

        else:
            fname = os.path.join(self.file_dir, 'water-level-data', fname)
            tide_df = pd.read_csv(fname)
            tide_df['t'] = pd.to_datetime(tide_df['t'])
            tide_df.index=tide_df['t']
            del tide_df['t']


        tide_df.index = tide_df.index.date
        return tide_df

    def combine_tide_slr(self, slr_df, slr_scenarios, tide_df, datums, nonexceendance_probs):
        combined_df = pd.DataFrame()
        combined_df.index = tide_df.index
        combined_df['tide_ft_MHHW'] = tide_df['v']  # from NOAA api, this is already in ft. and relative to MHHW

        for nonexceendance_prob in nonexceendance_probs:
            for source in slr_scenarios.keys():
                slr_source = slr_df.loc[slr_df['Source']==source]
                if source == 'NOAA et al. 2022':
                    slr_source = slr_source.loc[slr_source['Nonexceedence Probability']==nonexceendance_prob]

                for scenario in slr_scenarios[source]:
                    slr_ = slr_source.loc[slr_source['Scenario']==scenario].copy()
                    # print(datums.loc[slr_['Value Type'].unique()[0]])
                    datum_diff = datums.loc["MHHW"]['Value'] - datums.loc[slr_['Value Type'].unique()[0]]['Value']
                    slr_['SeaLevel_ft_MHHW'] = slr_["Sea Level (feet)"] - datum_diff
                    slr_ = slr_[['SeaLevel_ft_MHHW']]
                    slr_.index = slr_.index.date


                    missing_indices = combined_df.index.difference(slr_.index)
                    df_missing = pd.DataFrame(index=missing_indices)
                    df_missing['SeaLevel_ft_MHHW'] = np.nan

                    slr_ = pd.concat([slr_, df_missing])
                    
                    slr_ = slr_.sort_index()
                    slr_['SeaLevel_ft_MHHW'] = slr_["SeaLevel_ft_MHHW"].interpolate()

                    # print(slr_.head())
                    # print(len(slr_))
                    # print(combined_df.head())
                    # print(len(combined_df))
                    # print()
                    temp_df = pd.merge(combined_df, slr_, how='left', left_index=True, right_index=True)
                    scenario_name = 'SL_ft_MHHW_{}_ne{}' .format(scenario, nonexceendance_prob)
                    combined_df[scenario_name] = temp_df['SeaLevel_ft_MHHW']

                    scenario_name_w_tide = 'SL+Tide_ft_MHHW_{}_ne{}' .format(scenario, nonexceendance_prob)
                    combined_df[scenario_name_w_tide] = temp_df['SeaLevel_ft_MHHW'] + temp_df['tide_ft_MHHW']
        return combined_df
    
    def plot_tideSLR(self, savefig=False):
        for source in self.slr_scenarios.keys():
            fig, ax = plt.subplots(6,1,figsize=(12,6))
            # cmap = mpl.cm.get_cmap('Spectral')
            cmap = mpl.colormaps['Spectral']
            norm = mpl.colors.Normalize(vmin=0, vmax=5)

            xdata = self.combined_df.index
            # ax[0].set_
            ylim_min = []
            ylim_max = []

            ax[0].plot(xdata, self.combined_df['tide_ft_MHHW'], color='k', lw=0.1)
            ax[0].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            ax[0].yaxis.set_label_position("right")
            ax[0].set_ylabel("Tide Level", rotation=0, labelpad=30)

            ylim_min.append(ax[0].get_ylim()[0])
            ylim_max.append(ax[0].get_ylim()[1])            
            for scenario_i, scenario in enumerate(self.slr_scenarios[source]):
                color = cmap(norm(scenario_i+1))

                scenario_name = 'SeaLevel_ft_MHHW_{}_{}' .format(source.split(" ")[0], scenario)
                scenario_name_w_tide = 'SeaLevel+Tide_ft_MHHW_{}_{}' .format(source.split(" ")[0], scenario)

                ax[scenario_i+1].yaxis.set_label_position("right")
                ax[scenario_i+1].set_ylabel("{}m" .format(scenario), rotation=0, labelpad=20)

                ydata = self.combined_df[scenario_name]
                ax[scenario_i+1].plot(xdata, ydata, color=color, lw=3, zorder=1)

                ydata = self.combined_df[scenario_name_w_tide]
                ax[scenario_i+1].plot(xdata, ydata, color='0.7', lw=0.1, zorder=0)

                if scenario_i+1 < len(ax)-1:
                    ax[scenario_i+1].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                ylim_min.append(ax[scenario_i+1].get_ylim()[0])
                ylim_max.append(ax[scenario_i+1].get_ylim()[1])

            ylims = [np.min(ylim_min), np.max(ylim_max)]
            for scenario_i in range(len(self.slr_scenarios[source])+1):
                ax[scenario_i].set_xlim([datetime.date(2020, 1, 1), datetime.date(2080, 1, 1)])
                ax[scenario_i].set_ylim(ylims)

        
        y_label_text = "Water Level (ft; MHHW)\nStation - {}" .format(self.station_id)
        fig.text(0.04, 0.5, y_label_text, va='center', rotation='vertical')
        if savefig:
            fig_fn = os.path.join(self.file_dir, 'NOAA_SLR-Tide.png')
            plt.savefig(fig_fn, 
                        transparent=False, 
                        dpi=750,
                        bbox_inches='tight',
                        pad_inches=0.1,
                        )

            plt.close()

    def plot_SLR(self, savefig=False):
        for source in self.slr_scenarios.keys():
            fig, ax = plt.subplots(3, 1,figsize=(12,6), gridspec_kw={'height_ratios': [1, 2.5, 1]})
            cmap = mpl.colormaps['Spectral']
            norm = mpl.colors.Normalize(vmin=0, vmax=5)

            xdata = self.combined_df.index
            ylim_min = []
            ylim_max = []

            ax[0].plot(xdata, self.combined_df['tide_ft_MHHW'], color='k', lw=0.1)
            # ax[0].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
            ax[0].tick_params(labelbottom=False)    

            ax[0].yaxis.set_label_position("right")
            ax[0].set_ylabel("NOAA Tide\nPrediction",  rotation=0, labelpad=10, fontsize=10, ha='left', va='center')

            ylim_min.append(ax[0].get_ylim()[0])
            ylim_max.append(ax[0].get_ylim()[1])            
            for scenario_i, scenario in enumerate(self.slr_scenarios[source]):
                color = cmap(norm(scenario_i+1))
                scenario_name = 'SeaLevel_ft_MHHW_{}_{}' .format(source.split(" ")[0], scenario)

                ydata = self.combined_df[scenario_name]
                ax[1].plot(xdata, ydata, color=color, lw=3, zorder=1, label='{}m' .format(scenario))

                if scenario_i+1 < len(ax)-1:
                    ax[1].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                ylim_min.append(ax[1].get_ylim()[0])
                ylim_max.append(ax[1].get_ylim()[1])

            ax[1].yaxis.set_label_position("right")
            ax[1].set_ylabel("NOAA 2022\nSea Level Rise\nScenarios", rotation=0, labelpad=10, fontsize=10, ha='left', va='center')
            ax[1].legend()
            ylims = [np.min(ylim_min), np.max(ylim_max)]
            for i in range(len(ax)):
                ax[i].set_xlim([datetime.date(2020, 1, 1), datetime.date(2080, 1, 1)])

        ########################################################################
        color = cmap(norm(5))
        scenario_name_w_tide = 'SeaLevel+Tide_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
        scenario_name = 'SeaLevel_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
        
        ydata_wtide = self.combined_df[scenario_name_w_tide]
        ydata_slr = self.combined_df[scenario_name]
        ax[2].plot(xdata, ydata_slr, color=color, lw=3, zorder=1)
        ax[2].plot(xdata, ydata_wtide, color='0.7', lw=0.1, zorder=0)
        ax[2].yaxis.set_label_position("right")
        y_label_text = 'NOAA 2022\n2.0m Scenario\nw/ Tide'
        ax[2].set_ylabel(y_label_text, rotation=0, labelpad=10, fontsize=10, ha='left', va='center')



        ########################################################################
        color = cmap(norm(5))
        scenario_name_w_tide = 'SeaLevel+Tide_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
        scenario_name = 'SeaLevel_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
        
        ydata_wtide = self.combined_df[scenario_name_w_tide]
        ydata_slr = self.combined_df[scenario_name]
        ax[2].plot(xdata, ydata_slr, color=color, lw=3, zorder=1)
        ax[2].plot(xdata, ydata_wtide, color='0.7', lw=0.1, zorder=0)
        ax[2].yaxis.set_label_position("right")
        y_label_text = 'NOAA 2022\n2.0m Scenario\nw/ Tide'
        ax[2].set_ylabel(y_label_text, rotation=0, labelpad=10, fontsize=10, ha='left', va='center')


        ########################################################################
        y_label_text = "Water Level (ft; MHHW)\nStation - {}" .format(self.station_id)
        fig.text(0.05, 0.5, y_label_text, va='center', rotation='vertical')
        if savefig:
            fig_fn = os.path.join(self.file_dir, 'NOAA_SLR.png')
            plt.savefig(fig_fn, 
                        transparent=False, 
                        dpi=750,
                        bbox_inches='tight',
                        pad_inches=0.1,
                        )

            plt.close()

    def plot_inset(self, begindate_str, enddate_str, savefig=False):
        begindate = pd.to_datetime(begindate_str).date()
        enddate = pd.to_datetime(enddate_str).date()
        df = self.combined_df.loc[begindate:enddate]
        for source in self.slr_scenarios.keys():

            fig, ax = plt.subplots(1,1,figsize=(6,1.5))
            cmap = mpl.colormaps['Spectral']
            norm = mpl.colors.Normalize(vmin=0, vmax=5)
            color = cmap(norm(5))
            
            scenario_name_w_tide = 'SeaLevel+Tide_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
            scenario_name = 'SeaLevel_ft_MHHW_{}_{}' .format(source.split(" ")[0], 2.0)
            
            xdata = df.index
            ydata_wtide = df[scenario_name_w_tide]
            ydata_slr = df[scenario_name]
            # ax.plot(xdata, ydata_slr, color=color, lw=3, zorder=1)
            ax.plot(xdata, ydata_wtide, color='k', lw=0.1, zorder=0)
            ax.yaxis.set_label_position("right")
            # y_label_text = 'NOAA 2022\n2.0m Scenario\nw/ Tide'
            # ax.set_ylabel(y_label_text, rotation=0, labelpad=10, fontsize=10, ha='left', va='center')
            ax.set_xlim([begindate,enddate])
            ax.set_ylim([-1.5,4])

            ax.plot()

        if savefig:
            fig_fn = os.path.join(self.file_dir, 'NOAA_SLR_inset-{}-{}.png' .format(begindate_str, enddate_str))
            plt.savefig(fig_fn, 
                        transparent=False, 
                        dpi=750,
                        bbox_inches='tight',
                        pad_inches=0.1,
                        )

            plt.close()


    def define_slr_scenarios(self, scenarios):
        slr_scenarios = {
            "USACE 2013": ["Low", "Intermediate", "High"],
            "NOAA et al. 2022": ["0.3", "0.5", "1.0", "1.5", "2.0"],
            "NOAA Historical Data": ["NA"],
            "Projection based on Linear Trend of Monthly Data": ["NA"]
        }
        slr_scenarios = {i:slr_scenarios[i] for i in scenarios}
        return slr_scenarios

    def define_datums(self):
        """ Datum information for tide Station 8771450; all values are measured in feet.
        Datum information from https://tidesandcurrents.noaa.gov/datums.html?id=8771450
        
        """
        df = pd.DataFrame()
        df['Datum'] = ["MHHW", "MHW", "MTL", "MSL", "DTL", "MLW", "MLLW", "NAVD88", "STND", "GT", 'MN']
        df['Value'] = [1.41, 1.32, 0.82, 0.83, 0.71, 0.30, 0.00, 0.31, -4.38, 1.41, 1.02]       # value in feet
        df['Description'] = ["Mean Higher-High Water", "Mean High Water", "Mean Tide Level", 
                            "Mean Sea Level", "Mean Diurnal Tide Level", "Mean Low Water", 
                            "Mean Lower-Low Water", "North American Vertical Datum of 1988", 
                            "Station Datum", "Great Diurnal Range", "Mean Range of Tide"]
        df.set_index("Datum", inplace=True)
        return df


class NOAA_API:
    def __init__(self, station_id, begin_date, end_date, product="predictions", datum="MHHW", units="english", interval="hilo", time_zone="gmt"):
        self.file_dir = os.path.dirname(os.path.realpath(__file__))
        station = Station(id=station_id)
        self.df = station.get_data(
            begin_date=begin_date,
            end_date=end_date,
            product=product,
            datum=datum,
            units=units,
            interval=interval,
            time_zone=time_zone
            )
        self.df['year'] = self.df.index.year
        self.station_id = station_id
        self.begin_date = begin_date
        self.end_date = end_date
        self.datum = datum
        print("NOAA Tides Loaded")

    def plot(self):
        H_tide = self.df.loc[self.df['type']=="H"]
        xdata = self.df.index
        ydata = self.df['v']
        fig, ax = plt.subplots(1,1,figsize=(12,2))
        ax.plot(xdata, ydata, 'k', lw=0.3)
        # ax.plot(H_tide.index, H_tide['v'], color='r')

        ax.set_xlabel("Time")
        ax.set_ylabel("Water Level (ft; MHHW)\nStation - {}" .format(self.station_id))
    
    def get_max(self, year):
        max_yr = self.df.loc[self.df['year']==year].max()['v']
        print("{}: {}ft." .format(year, max_yr))

    def save_to_csv(self):
        fname = "NOAA_Tide_{}_MHHW_{}-{}.csv" .format(self.station_id, self.begin_date, self.end_date)
        fname = os.path.join(self.file_dir, 'output', fname)
        self.df.to_csv(fname)


if __name__ == "__main__":
    PTS = plot_TideSLR(
            station_id=8771450, 
            scenario_names=['NOAA et al. 2022'], # 'USACE 2013'], # Terri - use NOAA 2022; it's been approved by USACE too
            begin_date="20200101",
            end_date="20800101",
            # load_tides=False
        )
    # PTS.plot_tideSLR(savefig=False)
    # PTS.plot_SLR(savefig=True)
    PTS.plot_inset(begindate_str='20230101', enddate_str='20240101', savefig=False)
    plt.show()




















