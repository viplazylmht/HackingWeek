from utils import *

from typing import Union, Dict, List, Any, Optional, Tuple
from datetime import timedelta, date, datetime
import hashlib

from fastapi import Response, status
import operator

# Path: app/miniapp_journey.py

def build_root_node(start_date: date, granularity: Optional[Granularity], start_node: Optional[str] = None, device_os: Optional[str] = None) -> Dict[str, Any]:
    root_filter = {}
    date_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day,)

    if granularity == Granularity.DAILY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.DAILY.value))}
    elif granularity == Granularity.WEEKLY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.WEEKLY.value))}

    if start_node:
        root_filter['path.entity_name'] = start_node

    if device_os:
        root_filter['device_os'] = device_os

    root_node = {"filter": root_filter, "projection": { "_id": 0, "path.child.entity_name" if start_node else "path.entity_name": 1}}

    return root_node

def get_top_journeys_from_node(start_date: date, granularity: Optional[Granularity], start_node: Optional[str] = None, device_os: Optional[str] = None):

    date_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day,)

    # retrieve paths from cache

    top_journeys_cache_collection = journey_db["top_journeys_cache"]

    top_journeys = top_journeys_cache_collection.find_one({"date": date_time, "granularity": granularity.value, "start_node": start_node, "device_os": device_os})

    if top_journeys:
        return top_journeys['data']


    root_node = build_root_node(start_date, granularity, start_node, device_os)

    # TODO: retrieve paths from cache

    paths = find_all_path_from_node(miniapp_collection, root_node)

    # TODO: save paths to cache

    result = []

    for path, filters in add_tail_filter_to_paths(paths).items():
        path_stats = retrieve_journey_statistics(miniapp_collection, date_time, granularity, (path, filters))

        path_stats.update({"path_id": hashlib.sha256(path.encode('utf-8')).hexdigest()})

        result.append(path_stats)

    # sort the result by sessions, descending
    result.sort(key=lambda k: (k["stats"]["sessions"], k["stats"]["dist_users"]), reverse=True)

    # save top journeys to cache
    top_journeys_cache_collection.insert_one({"date": date_time, "granularity": granularity.value, "start_node": start_node, "device_os": device_os, "data": result})

    # result set limit to 1000
    return result[:1000]

def get_first_nodes(ds: date, granularity: Optional[Granularity], device_os: Optional[str] = None):

    date_time = datetime(year=ds.year, month=ds.month, day=ds.day,)
    # retrieve first nodes from cache
    first_node_cache_collection = journey_db["first_node_cache"]

    first_node = first_node_cache_collection.find_one({"date": date_time, "granularity": granularity.value, "device_os": device_os})

    if first_node:
        return first_node['data']

    root_filter = {}

    if granularity == Granularity.DAILY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.DAILY.value))}
    elif granularity == Granularity.WEEKLY:
        root_filter["journey_date"] = {"$gte": date_time, "$lt": date_time + timedelta(days=int(Granularity.WEEKLY.value))}

    if device_os:
        root_filter['device_os'] = {'$in': device_os.split(',')}

    root_node = {"filter": root_filter, "projection": { "_id": 0, "path.entity_name": 1}}

    cursor = miniapp_collection.find(**root_node)

    result = []

    for path in cursor:
        if path['path']['entity_name'] not in result:
            result.append(path['path']['entity_name'])

    # save first nodes to cache
    first_node_cache_collection.insert_one({"date": date_time, "granularity": granularity.value, "device_os": device_os, "data": result})

    return [{"node_name": node_name} for node_name in result]

def get_path_tree(start_date: date, granularity: Granularity,  node_name: Union[str, None] = None, depth: Union[int, None] = 0, device_os: Union[str, None] = None):

    date_time = datetime(year=start_date.year, month=start_date.month, day=start_date.day,)
    # retrieve paths from cache

    path_tree_cache_collection = journey_db["path_tree_cache"]

    path_tree = path_tree_cache_collection.find_one({"date": date_time, "granularity": granularity.value, "node_name": node_name, "depth": depth, "device_os": device_os})

    if path_tree:
        return Response(content=json.dumps(path_tree['data']), status_code=status.HTTP_200_OK, media_type="application/json")

    if not node_name and depth > 0:
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content="node_name is required if depth > 0")

    if depth < 0:
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content="depth must be >= 0")

    if node_name and depth == 0:
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content="depth must be > 0 if node_name is provided")

    root_node = build_root_node(start_date, granularity, None, device_os) # root node do not have node_name, pass None instead

    # TODO: retrieve paths from cache

    paths = add_tail_filter_to_paths(find_all_path_from_node(miniapp_collection, root_node))
    # TODO: save paths to cache

    sub_paths_dict = gen_sub_paths(paths, node_name, depth=depth)
    # print("DEBUG: sub_paths_dict: ", sub_paths_dict)

    if not sub_paths_dict:
        return Response(status_code=status.HTTP_404_NOT_FOUND, content= '{"message": "No path found"}', media_type="application/json")

    tree = build_tree(sub_paths_dict, is_root=(node_name is None))
    prepare_tree_filter(tree)
    collect_tree_stat(miniapp_collection, date_time, granularity, tree)

    transform_and_limit_tree(tree)

    result = tree

    # save path tree to cache
    path_tree_cache_collection.insert_one({"date": date_time, "granularity": granularity.value, "node_name": node_name, "depth": depth, "device_os": device_os, "data": result})

    return Response(content=json.dumps(result), status_code=status.HTTP_200_OK, media_type="application/json")
