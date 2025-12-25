import copy
import json
import traceback
from fastapi import FastAPI, HTTPException, status, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Any
from contextlib import asynccontextmanager
from asyncio import Lock
from value import Value
import httpx
import asyncio
import os
import clock
import time
import sys
import hashlib 
## LOCAL STATES ## 

local_view: Dict[int,str] = {} # the local view (for asgn4, this would be the view of the nodes within the shard), will remain in the old asgn3 format
global_view = {} # this is the view that has the entirety of the KVS in the new format

total_shards = None # Total number of shards across the KVS\
shard_id = None # The id of the shard this node belongs to, this is used to determine which shard a key belongs to
id_to_shard_name: Dict[int, str] = {} # Maps shard id to shard name 

kv_store: Dict[str, Value] = {} # the local kv store
local_clock: Dict[str, Dict[int, int]] = {}
client: httpx.AsyncClient = None
kv_lock: Lock = Lock() 

in_view_change = False
in_get = False

view_confirm = {}
start_gossip = False
gossiped = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, kv_store, local_clock, local_view, in_view_change, gossiped
    client = httpx.AsyncClient(timeout=5.0)
    stop_event = asyncio.Event()

    async def gossip_loop():
        while not stop_event.is_set():
            await asyncio.sleep(1)
            if not local_view or in_view_change or in_get or global_view is None:
                print("aaa", local_view, node_identifier, in_view_change, in_get, global_view, start_gossip)
                continue
            for sh_id in global_view:
                if sh_id == shard_id:
                    continue
                try:
                    for node in global_view[sh_id]:
                        payload = { "clock": {k: {kk: vv for kk, vv in v.items()} for k, v in local_clock.items()}
}
                        path = f"http://{node['address']}/global_gossip"
                        res = await client.put(path, json=payload)
                        if not res.status_code == 200:
                            print("Global Gossip code other than 200: ", res.status_code)
                        else:
                            gossiped = True
                            break
                except Exception as e:
                    traceback.print_exc()
                    print(f"Could not contact node {node['id']}: {e}", file=sys.stderr)
                    continue


            for peer in local_view:
                if not peer == node_identifier:
                    #wrapped this in try catch so it doesn't crash when a node can't be accessed
                    try:
                        path = f"http://{local_view[peer]}/gossip"

                        # async with kv_lock:
                        formatted_kv = {k: kv_store[k].to_dict() for k in kv_store}
                        res = await client.put(path, json={"id": node_identifier, "content": formatted_kv, "clock": local_clock})
                        if not res.status_code == 200:
                            print("Gossip code other than 200: ", res.status_code)
                    except Exception as e:
                        traceback.print_exc()
                        print(f"Could not contact node {peer}: {e}", file=sys.stderr)
                        continue

    task = asyncio.create_task(gossip_loop())

    yield

    stop_event.set()

    await task
    await client.aclose()

## BASE MODELS ##

class Node(BaseModel):
    address: str
    id: int
    
class View(BaseModel):
    view: List[Node]

class PutBody(BaseModel):
    value: str
    metadata: Dict[str, Dict[int, int]] = Field(alias="causal-metadata")

class GetBody(BaseModel):
    metadata: Dict[str, Dict[int, int]] = Field(alias="causal-metadata")
    class Config:
        populate_by_name = True

class Store(BaseModel):
    id: int
    content: Dict[str, Any] #Any is Value
    clock: Dict[str, Dict[int, int]]

class Clock(BaseModel):
    clock: Dict[str, Dict[int, int]]

app = FastAPI(lifespan=lifespan)
node_identifier: int = int(os.environ['NODE_IDENTIFIER'])

## HELPER FUNCTIONS ##

# Takes in key and total number of shards, returns shard_id
def hash_func(key : str, total_num_shards: int) -> int:
    hashed_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    r_hash = hashed_value % total_num_shards
    return r_hash


async def forward_to_node(method: str, path: str, json_body: str=None):
    try:
        result = None
        if method == "GET":
            result = await client.request(
                "GET",
                path,
                content=json.dumps(json_body),
                headers={"Content-Type": "application/json"}
    )
        elif method == "PUT":
            result = await client.put(path, json=json_body)
        
        return result
    except httpx.TimeoutException:
        raise HTTPException(status_code=408, detail="A timeout has occured.")
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while forwarding the request: {e}")

    
def serialize_store() -> Dict[str, Any]:
    result = {}
    for key, value in kv_store.items():
        result[key] = value.to_dict()
    return {"id": node_identifier, "content": result, "clock": local_clock}

def merge_store(store: Store, other_store: Store) -> None:
    for key in other_store.content:
        if key in store.content:

            if clock.vc_happens_before(store.clock[key], other_store.clock[key]):
                store.content[key].set_value(other_store.content[key].get_value())
                store.content[key].set_deps(other_store.content[key].get_deps())
                store.content[key].set_time(other_store.content[key].get_time())
                store.clock[key] = clock.vc_merge(store.clock[key], other_store.clock[key])

            elif clock.vc_is_concurrent(other_store.clock[key], store.clock[key]):
                t1 = store.content[key].get_time()
                t2 = other_store.content[key].get_time()
                if (t1 < t2) or (t1 == t2 and other_store.id > store.id):
                    store.content[key].set_value(other_store.content[key].get_value())
                    store.content[key].set_deps(other_store.content[key].get_deps())
                    store.content[key].set_time(other_store.content[key].get_time())
                    store.clock[key] = clock.vc_merge(store.clock[key], other_store.clock[key])

        else:
            t = other_store.content[key].get_time()
            store.content[key] = Value(other_store.content[key].get_value(), t)
            store.content[key].set_deps(other_store.content[key].get_deps())
            store.clock[key] = other_store.clock[key].copy()


## ENDPOINTS ##


@app.put("/global_gossip")
async def receive_gossip(data: Clock):
    global local_clock
    #print("Received global gossip with clock:", data.clock)
    #print("Current local clock:", local_clock)
    converted = {k: {int(inner_k): v for inner_k, v in inner_v.items()} for k, inner_v in data.clock.items()}
    for key in converted:
        if key not in local_clock:
            local_clock[key] = {}
        if not isinstance(converted[key], dict):
            raise HTTPException(status_code=400, detail="Invalid vector clock format.")
        local_clock[key] = clock.vc_merge(local_clock[key], converted[key])
    
    return JSONResponse(content={"message": "Vector clock received and merged"}, status_code=200)

@app.put("/gossip")
async def receive_gossip(store: Store):
    global kv_store, local_clock

    try:
        other_kv = {}
        for k in store.content:
            other_kv[k] = Value(store.content[k]["value"], store.content[k]["timestamp"])
            other_kv[k].set_deps(store.content[k]["dependencies"])
        
        other_store = Store(id=store.id, content=other_kv, clock=store.clock)
        this_store = Store(id=node_identifier, content=kv_store, clock=local_clock)

        merge_store(this_store, other_store)

        async with kv_lock:
            kv_store = this_store.content
            local_clock = this_store.clock
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error has occurred {e}.")
    
    
    return JSONResponse(content={"message": "Gossip received and merged"}, status_code=200)


@app.get("/ping", status_code=status.HTTP_200_OK)
async def ping():
    return "Node is initialized and ready to receive requests."


@app.get("/data/{key}")
async def get_value_at_key(key: str, body: GetBody):
    global in_get
    in_get = True

    print("GET request for key:", key, body.metadata)
    print("local clock", local_clock)
    print("metadata", kv_store)

    
    async with kv_lock:
        if not local_view or node_identifier not in local_view:
            in_get = False
            raise HTTPException(status_code=503)
    
    hash_for_key = hash_func(key, total_shards)
    print("hash_for_key", hash_for_key, "shard_id", shard_id)
    if hash_for_key == shard_id: 
        # execute the code below this if statement, basically if the hash for the incoming key matches the shard that this node is in,
        # then execute the PUT request like normal, if not, forward it to the correct shard (that is the else case). 
        # I haven't added the code for testing sake
        if body.metadata:
            client_clock = body.metadata if body and body.metadata else {}
        else:
            client_clock = {}

        if key == "":
            in_get = False
            raise HTTPException(status_code=400, detail="Invalid GET request.")
        
        """
        for dep in client_clock:
            if not dep == key:
                hashed_key = hash_func(dep, total_shards)
                if hashed_key == shard_id:
                    continue
                for node in global_view[hashed_key]:
                    dep_meta = {dep: client_clock[dep]}
                    path = f"http://{node["address"]}/share/{dep}"
                    try:
                        res = await client.get(path)
                        if res.status_code == 200:
                            body = res.json()
                            formatted_body = {int(k): v for k, v in body["clock"].items()}
                            
                            if not local_clock.get(dep):
                                local_clock[dep] = formatted_body
                            else:

                                local_clock[dep] = clock.vc_merge(local_clock[dep], formatted_body)
                            break
                        else:
                            print("Error fetching dependency metadata for key:", dep, res.status_code, res.text)
                    except Exception as e:
                        print(f"Error fetching dependency metadata for key {dep} from node {node['address']}: {e}", file=sys.stderr)
                        continue
        # print("client_clock", client_clock)
        # print("local_clock", local_clock)

        # for k in client_clock:
        #     if k not in local_clock:
        #         print(f"Key '{k}' is missing in local_clock. client_clock[{k}] = {client_clock[k]}")
        #     elif client_clock[k] != local_clock[k]:
        #         print(f"Key '{k}' differs:")
        #         print(f"  client_clock[{k}] = {client_clock[k]}")
        #         print(f"  local_clock[{k}] = {local_clock[k]}")
        """




                
        

        #async with kv_lock:
        # intialize clock to 0 if the key clock is not in metadata
        if key not in client_clock:
            client_clock[key] = {id: 0 for id in local_view}
        if key not in local_clock:
            local_clock[key] = {id: 0 for id in local_view}



        ready = clock.vc_happens_before(client_clock[key], local_clock[key]) or clock.vc_is_equal(client_clock[key], local_clock[key])
        waiting = not ready

        

        # need to verify that clock from client happens before node clock for key before servicing read
        if ready:
            #async with kv_lock:
            if key not in kv_store:
                in_get = False
                raise HTTPException(status_code=404, detail="The key does not exist in the store")
            
            
            for k in client_clock:
                if not k == key:
                    kv_store[key].add_dep(k, client_clock[k])

            # we only want to include the clocks for the keys that are causal dependencies
            new_meta = {key: copy.deepcopy(local_clock[key])}
            for dep in kv_store[key].get_deps():
                new_meta[dep] = kv_store[key].get_clock_at(dep)

            in_get = False
            return JSONResponse(content={"value": kv_store[key].get_value(), "causal-metadata": new_meta}, status_code=200)
            
        elif waiting:
            waited = 0
            updated = False

            while waited < 10:
                await asyncio.sleep(0.1)
                waited += 0.1

                async with kv_lock:
                    if clock.vc_happens_before(client_clock[key], local_clock[key]) or clock.vc_is_equal(client_clock[key], local_clock[key]):
                        updated = True
                        break
            
            if (updated):
                async with kv_lock:
                    for k in client_clock:
                        if not k == key:
                            kv_store[key].add_dep(k, client_clock[k])

                    if key in kv_store:
                        new_meta = {key: copy.deepcopy(local_clock[key])}
                        for dep in kv_store[key].get_deps():
                            new_meta[dep] = kv_store[key].get_clock_at(dep)

                        
                        in_get = False
                        return JSONResponse(content={"value": kv_store[key].get_value(), "causal-metadata": new_meta}, status_code=200)
                    else:
                        in_get = False
                        raise HTTPException(status_code=404, detail="The key does not exist in the store")
            else:
                    in_get = False
                    raise HTTPException(status_code=408, detail="Timeout occured when waitng for the key to be updated")
     
    else: 
        # go through the kvs_view and find the correct shard based on hash_for_key and forward this request to a node in that shard 
        # return to the client what the node in the correct shard responded 
        body_as_dict = body.model_dump(by_alias=True)        
        print("hash_for_key", hash_for_key, "shard_id", shard_id)

        res = None

        for node in global_view[hash_for_key]:
            
            path = f"http://{node["address"]}/data/{key}"
            res = await forward_to_node(method="GET", path=path, json_body=body_as_dict)
            if res.status_code == 200:
                in_get = False
                return JSONResponse(content=res.json(), status_code=res.status_code)

        if not res or res.status_code != 200:
            in_get = False
            raise HTTPException(status_code=res.status_code, detail="An error has occurred fetching the values..")
    


@app.put("/data/{key}")
async def put_value_at_key(key: str, body: PutBody):
    
    async with kv_lock:
        if not local_view or node_identifier not in local_view:
            raise HTTPException(status_code=503)

    hash_for_key = hash_func(key, total_shards)

    if hash_for_key == shard_id: 
        # execute the code below this if statement, basically if the hash for the incoming key matches the shard that this node is in,
        # then execute the PUT request like normal, if not, forward it to the correct shard (that is the else case). 
        # I haven't added the code for testing sake
        if body.metadata:
            client_clock = body.metadata if body and body.metadata else {}
        else:
            client_clock = {}

        if key == "":
            raise HTTPException(status_code=400, detail="Invalid PUT request.")
                
        

        async with kv_lock:
            if key not in client_clock:
                client_clock[key] = {id: 0 for id in local_view}

            if key not in local_clock:
                local_clock[key] = {id: 0 for id in local_view}

            # if the clock from the causal metadata from the key is behind, we update it with the key clock
            if clock.vc_happens_before(local_clock[key], client_clock[key]):
                local_clock[key] = clock.vc_merge(local_clock[key], client_clock[key])
                
            # increment the node's clock
            clock.vc_increment(local_clock[key], node_identifier)

            if key not in kv_store:
                kv_store[key] = Value(body.value)
            else:
                kv_store[key].set_value(body.value)

            kv_store[key].set_time(time.time())

            for k in client_clock:
                if not k == key:
                    kv_store[key].add_dep(k, client_clock[k])



            # we only want to include the clocks for the keys that are causal dependencies
            new_meta = client_clock.copy()
            new_meta[key] = local_clock[key]
            for dep in kv_store[key].get_deps():
                new_meta[dep] = kv_store[key].get_clock_at(dep)

            return JSONResponse(content={"causal-metadata": new_meta}, status_code=200)
     
    else: 
        # go through the kvs_view and find the correct shard based on hash_for_key and forward this request to a node in that shard 
        # return to the client what the node in the correct shard responded 
        # Format of kvs_view => {shard1: [], shard2: []}
        body_as_dict = body.model_dump(by_alias=True)
        
        res = None


        for node in global_view[hash_for_key]:
            
            path = f"http://{node["address"]}/data/{key}"
            res = await forward_to_node(method="PUT", path=path, json_body=body_as_dict)
            if res.status_code == 200:
                return JSONResponse(content=res.json(), status_code=res.status_code)

        if not res or res.status_code != 200:
            raise HTTPException(status_code=res.status_code, detail="An error has occurred fetching the values..")






    

@app.get("/data")
async def get_all(body: GetBody):
    items = {}

    if body.metadata:
        meta = body.metadata if body and body.metadata else {}
    else:
        meta = {}

    async with kv_lock:
        if not local_view or node_identifier not in local_view:
            raise HTTPException(status_code=503)
    
    combined_meta = {**meta, **local_clock}

    for key in combined_meta:
        hash_for_key = hash_func(key, total_shards)
        if hash_for_key != shard_id: 
            continue
        res = await get_value_at_key(key, GetBody(metadata=meta))
        
        if not res.status_code == 200:
            raise HTTPException(status_code=res.status_code, detail="An error has occurred fetching the values.")
        
        content = res.body
        # content = res.json()
        value = json.loads(content)["value"]
        # value = content["value"]
        new_meta = json.loads(content)["causal-metadata"]
        #new_meta = content["causal-metadata"]


        items[key] = value

    meta = copy.deepcopy(new_meta)

    return JSONResponse(content={"items": items, "causal-metadata": meta}, status_code=200)


@app.put("/view")
async def update_view(request: Request):
    try:
        global local_view, local_clock, global_view, total_shards, shard_id, id_to_shard_name, in_view_change, kv_lock, start_gossip, view_confirm, gossiped
        in_view_change = True
        start_gossip = False
        gossiped = False
        if view_confirm.get(node_identifier) is True:
            view_confirm = {}


        body = await request.json()


        sorted_shard_names = sorted(body["view"])
        new_global_view = {}
        new_id_to_shard_name = {}
        new_shard_id = None
        
        for shard_name in sorted_shard_names:
            for node in body["view"][shard_name]:
                if view_confirm.get(node["id"]) is None:
                    view_confirm[node["id"]] = False        

        this_shard_nodes = []
        for id, shard_name in enumerate(sorted_shard_names):
            node_list = body["view"][shard_name]
            new_global_view[id] = node_list
            new_id_to_shard_name[id] = shard_name
            for node in node_list:
                if node["id"] == node_identifier:
                    this_shard_nodes = node_list
                    new_shard_id = id
                    break

        new_local_view = {node["id"]: node["address"] for node in this_shard_nodes}


        # Parse the body of the PUT request (should be full KVS View)

        # 3 scenarios:
        # 1. we are being added to an existing shard and all the shards are staying the same - just gossip
        # 2. we are being added to an existing shard and the shards are changing - reshard first by redistributing keys, 
        #    move keys to new nodes, then gossip and delete old keys
        # 3. we are being added to a new shard - same as above
        # 4. we stay in our shard, but another shard is removed causing redstiribution of keys

        old_nodes = {}
        for nodes in global_view.values():
            for node in nodes:
                old_nodes[node["id"]] = node["address"]

        new_nodes = {}
        for nodes in new_global_view.values():
            for node in nodes:
                new_nodes[node["id"]] = node["address"]

        removed_node_ids = set(old_nodes.keys()) - set(new_nodes.keys())

        for node_id in removed_node_ids:
            address = old_nodes[node_id]
            try:
                res = await client.get(f"http://{address}/move")
                if res.status_code == 200:
                    #hash each key, and if the hash matches the shard this node is in, then save it to the kv_store and local_clock
                    data = res.json()
                    for key, value in data["keys"].items():
                        hs = hash_func(key, len(new_global_view))
                        if hs == new_shard_id:
                            kv_store[key] = Value(value["value"], value["timestamp"])
                            kv_store[key].set_deps(value["dependencies"])
                            formatted_data = {int(k): v for k, v in data["clock"][key].items()}
                            local_clock[key] = formatted_data

                else:
                    print(f"Failed to GET /data from removed node {node_id} at {address}: {res.status_code}")

            except Exception as e:
                print(f"Failed to GET /data from removed node {node_id} at {address}: {e}")

        
        old_view = copy.deepcopy(global_view) if global_view else {} # Save the old view for resharding logic

        async with kv_lock:
            global_view = new_global_view     # New format: { "Shard1": [...], "Shard2": [...] } ; Save the full system view
            id_to_shard_name = new_id_to_shard_name
            total_shards = len(global_view)    # Count the total number of shards (used later for hash function)
            shard_id = new_shard_id    # Save the shard id this node belongs to

            # Replace old shard view with new view
            local_view.clear()
            local_view.update(new_local_view)

            # Add missing clocks entries for new nodes in shard (don't delete old ones to preserve causality)
            for key in local_clock:
                # #makes sure to delete nodes that got removed from view
                # for node_id in list(local_clock[key]):
                #     if node_id not in view:
                #         del local_clock[key][node_id]
                #makes sure to add nodes that got added to view
                for node_id in local_view:
                    if node_id not in local_clock[key]:
                        local_clock[key][node_id] = 0
            
               # Prep to sync kv_store to other nodes in shard


        
        # resharding 
        # this if statement checks if there is already a view previously. If this is the first view request, no need for resharding
        if len(old_view) != 0:

            # check if there is difference between set of old shards and the set of new shards 
            if set(new_global_view.keys()) != set(old_view.keys()):
                new_total_shards = len(new_global_view)
                for key in list(kv_store.keys()): 
                    # redo hash with new number of shards
                    hs = hash_func(key, new_total_shards)
                    if hs == new_shard_id:
                        continue
                    key_info = {
                        "value": kv_store[key].to_dict(),
                        "clock": local_clock[key]
                    }

                    # go through all nodes in that shard and do a PUT request with the key
                    sent = False 

                    while True:
                        await asyncio.sleep(0.1)
                        for node in new_global_view[hs]:
                            path = f"http://{node["address"]}/move/{key}"
                            res = await client.put(path, json=key_info)
                            if res.status_code == 200:
                                del kv_store[key]
                                sent = True
                                break

                        if sent:
                            break

        #check if all other nodes in view_confirm have confirmed the view change except for this node
        last_node = all(val for node_id, val in view_confirm.items() if node_id != node_identifier)
        print(last_node)

        for id in new_global_view:
            for node in new_global_view[id]:
                path = f"http://{node['address']}/confirm_view"
                print(node_identifier, last_node, path)
                res = await client.put(path, json={"id": node_identifier, "last_node": last_node})

        if last_node:
            start_gossip = True
            await asyncio.sleep(5)  # Give some time for gossip to start

        view_confirm[node_identifier] = True
        print(start_gossip)
        

       


        
        return {
            "message": "View updated successfully",
            "shard_id": shard_id,
            "total_shards": total_shards,
            "view": local_view,
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"An error has occurred while updating the view: {e}")
    finally:

        in_view_change = False

@app.put("/move/{key}")
async def move_put_key(key: str, request: Request):
    #request will be: content: , clock: 
    body = await request.json()
    content:dict = body.get("value", {})
    clock = body.get("clock")

    if not key or not clock or not content:
        raise HTTPException(status_code=400, detail="Invalid request body. Must contain 'key' and 'clock'.")
    
    async with kv_lock:
        kv_store[key] = Value(value=content["value"], timestamp=content["timestamp"])
        kv_store[key].set_deps(content["dependencies"])
        local_clock[key] = clock
    return JSONResponse(content={"message": "Key moved successfully"}, status_code=200)
    
@app.get("/move")
async def move_get_key(request: Request):
    global kv_store, local_clock, kv_lock, local_view, global_view
    async with kv_lock:
        keys = {k: kv_store[k].to_dict() for k in kv_store}
        local_view = {}
        global_view = {}
        return JSONResponse(content={"keys": keys, "clock": local_clock}, status_code=200)
    
@app.get("/share/{key}")
async def share_causal(key: str, request: Request):
    return JSONResponse(content={"clock": local_clock.get(key, {})}, status_code=200)

@app.put("/confirm_view")
async def confirm_view(request: Request):
    global local_view_confirm, start_gossip, gossiped

    restart = True
    for node_id in view_confirm:
        if view_confirm[node_id] is False:
            restart = False
            break
    if restart:
        view_confirm.clear()
        start_gossip = False
        gossiped = False

    body = await request.json()
    node_id = body.get("id")

    if view_confirm == {} or view_confirm is None:
        view_confirm[node_identifier] = False
    
    view_confirm[node_id] = True
    if (body.get("last_node", False) == True):
        start_gossip = True
    return JSONResponse(content={"ok": True}, status_code=200)


    