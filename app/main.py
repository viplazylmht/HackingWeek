from typing import Union, Annotated, List

from fastapi import FastAPI, Response, Path, Query

from openapi_tags import tags_metadata
from datetime import date

from utils import Granularity, TopPath, Message, FirstNode, PathTree
from miniapp_journey import get_top_journeys_from_node, get_first_nodes, get_path_tree
import json

app = FastAPI(openapi_tags=tags_metadata)

@app.get("/ping", tags=["ping"])
def ping_pong():
    return "pong!"

@app.get("/journeys/first_nodes/{ds}/{granularity}", tags=["miniapp journey table"],  response_model=List[FirstNode],
    responses={
        200: {
            "description": "First nodes retrieved successfully with any order.",
            "content": {
                "application/json": {
                    "example": [{"node_name":"First miniapp"},{"node_name":"Zero miniapp"}]
                }
            }
        }
    })
def first_nodes(ds: date, granularity: Granularity, device_os: Union[str, None] = None):
    result = get_first_nodes(ds, granularity, device_os)
    return Response(content=json.dumps(result), media_type="application/json")

@app.get("/journeys/top_paths/{ds}/{granularity}", tags=["miniapp journey table"], response_model=List[TopPath],
    responses={
        404: {"model": Message, "description": "The item was not found"},
        200: {
            "description": "Paths retrieved successfully and statistics calculated, sorted by the number of sessions.",
            "content": {
                "application/json": {
                    "example": [{"path": "First miniapp.Second miniapp.Third miniapp.Fourth miniapp", "stats": {"dist_users": 2, "sessions": 4, "device_os": [{"_id": "IOS", "sessions": 2, "dist_users": 2}, {"_id": "Android", "sessions": 2, "dist_users": 1}], "previous_sessions": 0, "previous_dist_users": 0}, "path_id": "a0a7aa8bbddd90cc1351f2f08729fcd3d74a42b123a542eb7c2b515c6f4a3b06"}, {"path": "First miniapp.Second miniapp.Third miniapp.Fourth miniapp.Five miniapp", "stats": {"dist_users": 1, "sessions": 1, "device_os": [{"_id": "Android", "sessions": 1, "dist_users": 1}], "previous_sessions": 0, "previous_dist_users": 0}, "path_id": "addade6906aaab08ebefd250e3d74c716b363f676f1b15840f9f10ccd736d253"}, {"path": "First miniapp.Second miniapp.Fourth miniapp", "stats": {"dist_users": 1, "sessions": 1, "device_os": [{"_id": "Android", "sessions": 1, "dist_users": 1}], "previous_sessions": 0, "previous_dist_users": 0}, "path_id": "7e3f36484a45bcf1dd7a94914cee109d3a379563ded0bac9823ed6842b3c33a7"}]
                }
            },
        },
})
def top_paths(ds: date, granularity: Granularity,  start_node: Union[str, None] = None, device_os: Union[str, None] = None):
    # return {"ds": ds, "granularity": granularity, "start_node": start_node, "device_os": device_os}

    result = get_top_journeys_from_node(ds, granularity, start_node, device_os)

    return Response(content=json.dumps(result), media_type="application/json")

@app.get("/journeys/tree/{ds}/{granularity}", tags=["miniapp journey tree"], response_model=PathTree,
    responses={
        404: {"model": Message, "description": "The item was not found"},
        200: {
            "description": "Tree retrieved successfully and statistics calculated.",
            "content": {
                "application/json": {
                    "example": {"name":"root","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"First miniapp":{"name":"First miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Second miniapp":{"name":"Second miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Third miniapp":{"name":"Third miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Fourth miniapp":{"name":"Fourth miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Five miniapp":{"name":"Five miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{}}}}}},"Fourth miniapp":{"name":"Fourth miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{}}}}}},"Zero miniapp":{"name":"Zero miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"First miniapp":{"name":"First miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Third miniapp":{"name":"Third miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{"Fourth miniapp":{"name":"Fourth miniapp","stats":{"dist_users":0,"sessions":0,"device_os":[]},"children":{}}}}}}}}}},
                }
            },
    }},)
def get_tree(ds: date, granularity: Granularity,  node_name: Union[str, None] = None, depth: Union[int, None] = 0, device_os: Union[str, None] = None):
    return get_path_tree(ds, granularity, node_name, depth, device_os)

