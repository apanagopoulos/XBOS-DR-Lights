import time
import msgpack
import datetime
from xbos import get_client
from xbos.devices.demand_response import Demand_Response
import subprocess
import shlex

uri = "ciee/*/i.xbos.demand_response/signal/info"

def _handle(msg):
    for po in msg.payload_objects:
        if po.type_dotted == (2,1,1,9):
            data = msgpack.unpackb(po.content)
            event_start = datetime.datetime.fromtimestamp(data['event_start']/1e9)
            event_end = datetime.datetime.fromtimestamp(data['event_end']/1e9)
            status = ['not configured', 'unusable', 'inactive', 'active'][data['dr_status']]
            eventtype = ['no event','normal','moderate','high','special'][data['event_type']]
            if data['event_start'] > 0:
                print "EVENT starts at", event_start
                print "Status:", status
                print "Type:", eventtype
				        # call function --- write config file
            else:
                print "No event scheduled"

c = get_client()
c.subscribe(uri, _handle)
while True:
    time.sleep(1000)
