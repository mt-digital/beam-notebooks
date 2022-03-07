import pandas as pd
import os

from os.path import join as pjoin
from glob import glob


def build_events_df(datadir):

    event_csvs = glob(pjoin(datadir, "ITERS/it.*/*.events.csv*"))

    event_iter_dfs = [
        (
            int(os.path.split(event_csv)[-1][0]),
            pd.read_csv(event_csv, low_memory = False)
        )
        for event_csv in event_csvs
    ]

    for idx, tup in enumerate(event_iter_dfs):
        event_iter_dfs[idx][1]["iter"] = tup[0]

    ret = pd.concat(
        [el[1] for el in event_iter_dfs]
    )

    return ret


def build_charging_df(
        datadir,
        select_cols=["iter", "time", "type", "chargingPointType",
                     "parkingType", "vehicle", "primaryFuelLevel", "vehicleType", "capacity"]
    ):

    events_df = build_events_df(datadir)

    ret = events_df[select_cols].sort_values(
        ["iter", "time"]
    )

    return ret[
        ret.type.isin(
            ["ChargingPlugInEvent", "ChargingPlugOutEvent", "RefuelSessionEvent"]
        )
    ]
