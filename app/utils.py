from pymongo import MongoClient
import urllib.parse
from bson.son import SON
from datetime import datetime, date
from bson.objectid import ObjectId

from  pymongo.collection import Collection
import copy
import re
import json
import os
from typing import Union, Dict, List, Any, Optional, Tuple

from datetime import timedelta
from enum import Enum

class Granularity(str, Enum):
    DAILY = 1
    WEEKLY = 7
from functools import partial

from pydantic import BaseModel, Field

class DeviceOS(BaseModel):
    id: str = Field(description="device os", alias="_id")
    sessions: int = Field(description="number of sessions")
    dist_users: int = Field(description="number of distinct users")

class Stats(BaseModel):
    dist_users: int = Field(description="number of distinct users")
    sessions: int = Field(description="number of sessions")
    device_os: Optional[List[DeviceOS]] = Field(description="List of device os, order by sessions desc, dist_users desc, _id asc")

class TopPath(BaseModel):
    path: str = Field(description="path of the journey, punctuated by '.'")
    path_id: str = Field(description="sha256 hash of the path")
    stats: Stats = Field(description="statistics of the journey")

class Message(BaseModel):
    message: str = Field(description="message of the response")

class FirstNode(BaseModel):
    node_name: str = Field(description="name of the first node")

class PathTree(BaseModel):
    name: str = Field(description="name of the node unless this is the root of the tree")
    depth: int = Field(description="depth of the node in the base tree")
    stats: Stats = Field(description="statistics of the node in the tree")
    children: Optional[Dict[str, "PathTree"]] = Field(description="children of the path. Each child is a keypair of the name of the child and the child itself")

URI = 'mongodb://root:example@mongo:27017/'

client = MongoClient(URI)
journey_db = client.journey_db

miniapp_collection = journey_db.miniapp2

def make_miniapp_journey(agent_id, journey_id, journey_date, device_os, path=None):
    """
    agent_id: int: user_id
    journey_id: str or bytes: id of the user journey
    journey_date: date:
    device_os: str: platform
    path: nested dict: path of this journey
    """

    return SON(agent_id=agent_id, journey_id=journey_id, journey_date=journey_date, device_os=device_os, path=path)

def make_miniapp_node(entity_id, entity_name, child=None):
    """
    entity_id: int: the identity of the entity
    entity_name: str: name of the entity
    child: the child entity
    """

    return SON(entity_id=entity_id, entity_name=entity_name, child=child)

def build_filter_and_projection(projection) -> Dict:
    key_builder = []
    projection_builder = []
    value = None

    while isinstance(projection, dict):
        if "path" in projection:
            key_builder.append("path")
            projection_builder.append("path")
            projection = projection["path"]
        elif "child" in projection:
            key_builder.append("child")
            projection_builder.append("child")
            projection = projection["child"]
        elif "entity_name" in projection:
            key_builder.append("entity_name")
            projection_builder.append("child.entity_name")
            value = projection["entity_name"]
            projection = None
        else:
            # invalid path
            return None

    return {
        "next_filter": {".".join(key_builder): value},
        "next_projection_key": ".".join(projection_builder)
    }

def current_journey(filter):
    path = []

    key_builder = ["path", "entity_name"]
    while ".".join(key_builder) in filter:
        path.append(filter[".".join(key_builder)])
        key_builder.insert(1, 'child')

    return ".".join(path)

root_node = {"filter": {
    "journey_date": { "$gte": datetime(2023, 6, 10, 0, 0) },
    "device_os": {"$in": ["IOS", "Android"]},
}, "projection": { "_id": 0, "path.entity_name": 1}}


def find_all_path_from_node(collection: Collection, node):
    journey_paths = []
    return find_all_path_from_node_re(collection, node, journey_paths)

def find_all_path_from_node_re(collection: Collection, node, journey_paths):
    result = {}

    cr_journey = current_journey(node["filter"])
    if cr_journey in journey_paths:
        # print("current_journey", cr_journey)
        return {}
    else:
        # print("add to journey paths:", cr_journey)
        journey_paths.append(cr_journey)

    cursor = collection.find(**node)

    for path in cursor:
        # print(path)
        next = build_filter_and_projection(path)

        if next:
            next_node = copy.deepcopy(node)
            next_node["filter"].update(**next["next_filter"])
            next_node["projection"] = { "_id": 0, next["next_projection_key"]: 1}

            result.update(find_all_path_from_node_re(collection, next_node, journey_paths))
        else:
            if cr_journey not in result:
                result.update({cr_journey: node["filter"]})

    return result

def gen_child_base_filter(depth: int = 0):
    return ".".join(["path"] + ["child" for i in range(depth)])

def gen_child_entity_name_filter(depth: int = 0):
    return ".".join(["path"] + ["child" for i in range(depth)] + ["entity_name"])

def add_tail_filter_to_path(path, filter):
    filter.update({
        gen_child_base_filter(depth=len(path.split("."))): {"$type": 10}
    })

    return filter

def add_tail_filter_to_paths(paths):
    return {k: add_tail_filter_to_path(k, v) for k, v in paths.items()}

def retrieve_statistic_from_filter(collection, filter):
    pipeline = [
        {"$match": filter},
        { "$facet":
            {
                "dist_users":
                    [
                        { "$group" : { "_id" : "$agent_id"}}
                    ],
                 "sessions":
                    [
                        {"$count": "count"}
                    ]}
        },
        { "$addFields":
            { "dist_users":
                {"$size": "$dist_users"},
            "sessions": { "$first": "$sessions.count" }
            }
        }
    ]

    result = {}
    for i in collection.aggregate(pipeline):
        result = i
        break

    device_os_pipeline = [
        {"$match": filter},
        {"$group": {"_id": {"device_os": "$device_os", "agent": "$agent_id"}, "count_session": {"$sum": 1}}},
        # {"$sort": { "count_session": -1}},
        {"$group": {"_id": "$_id.device_os",
                  "sessions": {"$sum":"$count_session"}, "dist_users": {"$sum": 1}}},
        {"$sort": SON([("sessions", -1), ("dist_users", -1), ("_id", 1)])},
    ]

    result["device_os"] = list(collection.aggregate(device_os_pipeline))


    return result

def comparing_stat(cur_stat, pre_stat):
    if "sessions" not in cur_stat:
        cur_stat["sessions"] = 0

    cur_stat["previous_sessions"] = pre_stat.get("sessions", 0)
    cur_stat["previous_dist_users"] = pre_stat.get("dist_users", 0)

    return cur_stat

def retrieve_journey_statistics(collection: Collection, start_date: datetime, granularity: Granularity, path_item):
    end_date = start_date + timedelta(days=int(granularity))

    previous_start_date = start_date - timedelta(days=int(granularity))

    path = path_item[0]
    path_filter = path_item[1]

    cur_filter = copy.deepcopy(path_filter)
    cur_filter.update({"journey_date": { "$gte": start_date, "$lt": end_date }})

    cur_statistic = retrieve_statistic_from_filter(collection, cur_filter)

    pre_filter = copy.deepcopy(path_filter)
    pre_filter.update({"journey_date": { "$gte": previous_start_date, "$lt": start_date }})
    pre_statistic = retrieve_statistic_from_filter(collection, pre_filter)


    return { "path": path, "stats": comparing_stat(cur_statistic, pre_statistic) }

def gen_sub_paths(all_paths, anchor_node_name=None, depth=0):

    parent_pattern = ''.join(['[^.]+?\.' for i in range(depth-1)])
    pattern = "^()([^.]+?(?:$|\.).*)" if depth < 1 else f"^({parent_pattern})({anchor_node_name}(?:$|\.).*)"

    # print("DEBUG: pattern", pattern)
    matcher = re.compile(pattern)

    sub_path_to_paths = {}
    for p, f in all_paths.items():

        if anchor_node_name:
            match = matcher.match(p)

            if match:
                [parent_path, sub_path] = match.groups()

                parent_strip = parent_path.strip(".")
                position = len(parent_strip.split(".")) if parent_strip else 0
                depth = position + 1

                if sub_path not in sub_path_to_paths:
                    sub_path_to_paths[sub_path] = [(p, position, depth)]
                else:
                    sub_path_to_paths[sub_path].append((p, position, depth))
        else:
            sub_path_to_paths[p] = [(p, 0, 1)]

    return sub_path_to_paths

def add_node_to_tree(sub_tree: dict, path: str, starting_point: int, depth: int):

    extract_node, remaining_nodes = None, None

    if "." in path:
        extract_node = path[:path.index(".")]
        remaining_nodes = path[path.index(".")+1:]

    else:
        extract_node = path

    sub_tree["name"] = extract_node
    sub_tree["depth"] = depth

    if "starting_points" in sub_tree:
        if starting_point not in sub_tree["starting_points"]:
            sub_tree["starting_points"].append(starting_point)
    else:
        sub_tree["starting_points"] = [starting_point,]

    if "children" not in sub_tree:
        sub_tree["children"] = {}

    if remaining_nodes:
        next_node = remaining_nodes[:remaining_nodes.index(".")] if "." in remaining_nodes else remaining_nodes

        if next_node not in sub_tree["children"]:
            sub_tree["children"][next_node] = {}
        add_node_to_tree(sub_tree["children"][next_node], remaining_nodes, starting_point, depth+1)


def build_tree(sub_path_dict, is_root=False):
    tree = {}

    if is_root:
        tree["name"] = "root"
        tree["starting_points"] = [-1,]
        tree["children"] = {}
        tree["depth"] = 0

    for sp, fpd in sub_path_dict.items():

        if is_root:
            start_node = sp[:sp.index(".")] if "." in sp else sp

            if start_node not in tree["children"]:
                tree["children"][start_node] = {}

        for fp, p, d in fpd:

            if is_root:
                add_node_to_tree(tree["children"][start_node], sp, p, d)
            else:
                add_node_to_tree(tree, sp, p, d)

    return tree

def prepare_tree_filter(tree: dict, parents=[]):

    if ("starting_points" not in tree) or "name" not in tree:
        return

    filters = []

    for starting_point in tree["starting_points"]:
        filter = {}
        for s, parent_node in enumerate(parents, starting_point):
            filter[gen_child_entity_name_filter(depth=s)] = parent_node

        filter[gen_child_entity_name_filter(depth=starting_point+len(parents))] = tree["name"]

        filters.append(filter)

    tree["filter"] = {"$or": filters}

    del tree["starting_points"]

    if "children" in tree:
        for k in tree["children"].keys():
            prepare_tree_filter(tree["children"][k], parents + [tree["name"],])

def collect_tree_stat_rec(recv_fn, tree: dict):
    result = recv_fn((tree["name"], tree["filter"]))

    del tree["filter"]
    tree["stats"] = result.get("stats")

    if "children" in tree:
        for k in tree["children"].keys():
            collect_tree_stat_rec(recv_fn, tree["children"][k])

def collect_tree_stat(collection: Collection, start_date: datetime, granularity: Granularity, tree: dict):
    retrieve_journey_statistics_fn = partial(retrieve_journey_statistics, collection, start_date, granularity)
    collect_tree_stat_rec(retrieve_journey_statistics_fn, tree)
