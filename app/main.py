from typing import Union

from fastapi import FastAPI
from openapi_tags import tags_metadata

app = FastAPI(openapi_tags=tags_metadata)

@app.get("/", tags=["root"])
def read_root():
    return {"Hello": "World"}

@app.get("/ping", tags=["ping"])
def ping_pong():
    return "pong!"

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}