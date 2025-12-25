#Vector Clock code
#use dictionary and store keys as string, so we don't have to worry about ordring ints, etc.

def vc_increment(vc: dict, node_id: str):
   event_count = vc[node_id]
   vc[node_id] = event_count + 1


def vc_happens_before(this_vc: dict, other_vc: dict) -> bool:
    less_than = False

    clean_this = {k: v for k, v in this_vc.items() if v != 0}
    clean_other = {k: v for k, v in other_vc.items() if v != 0}

    if len(clean_this) == 0 and len(clean_other) == 0:
        return False

    for node_id in clean_this:
        this_event_count = clean_this[node_id]
        if this_event_count == 0:
            continue
        #default this to 0 if other vc hasn't seen node
        other_event_count = clean_other.get(node_id, 0)
        if this_event_count < other_event_count:
            less_than = True
        elif this_event_count > other_event_count:
            return False
    
    return less_than

def vc_is_equal(this_vc: dict, other_vc: dict) -> bool:
    clean_this = {k: v for k, v in this_vc.items() if v != 0}
    clean_other = {k: v for k, v in other_vc.items() if v != 0}
    return clean_this == clean_other


def vc_is_concurrent(this_vc: dict, other_vc: dict) -> dict:
    return not vc_happens_before(this_vc, other_vc) and not vc_happens_before(other_vc, this_vc)

def vc_merge(this_vc: dict, other_vc: dict) -> dict:
   new_vc = this_vc.copy()
   for node_id in other_vc:
       this_event_count = this_vc.get(node_id, 0) #the get(node_id, 0) gives us a default value of 0 if node_id DNE in this_vc
       other_event_count = other_vc[node_id]
       new_vc[node_id] = max(this_event_count, other_event_count)
   return new_vc
