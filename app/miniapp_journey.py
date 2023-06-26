from utils import *

from typing import Union, Dict, List, Any, Optional, Tuple
from datetime import timedelta, date, datetime
import hashlib

# Path: app/miniapp_journey.py

def get_top_journeys_from_node(start_date: date, granularity: Optional[Granularity], start_node: Optional[str] = None, device_os: Optional[str] = None):

    root_filter = {}
    date_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day,)

    if granularity == Granularity.DAILY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.DAILY.value))}
    elif granularity == Granularity.WEEKLY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.WEEKLY.value))}

    if start_node:
        root_filter['path.entity_name'] = start_node

    if device_os:
        root_filter['device_os'] = {'$in': device_os.split(',')}

    root_node = {"filter": root_filter, "projection": { "_id": 0, "path.child.entity_name" if start_node else "path.entity_name": 1}}

    # TODO: retrieve paths from cache

    paths = find_all_path_from_node(miniapp_collection, root_node)

    # TODO: save paths to cache

    result = []

    for path, filters in add_tail_filter_to_paths(paths).items():
        path_stats = retrieve_journey_statistics(miniapp_collection, date_time, granularity, (path, filters))

        path_stats.update({"path_id": hashlib.sha256(path.encode('utf-8')).hexdigest()})

        result.append(path_stats)

    return result
