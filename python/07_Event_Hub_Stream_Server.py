%python

dbutils.widgets.text("CONNECTION_STRING", "", "Connection String")
dbutils.widgets.text("EVENT_HUB_NAME", "wiki-changes", "Event Hub")
dbutils.widgets.text("EVENT_COUNT","1000","Event Count")

%python

pcString = dbutils.widgets.get("CONNECTION_STRING")
uniqueEHName = dbutils.widgets.get("EVENT_HUB_NAME")

# set this to number of records you want
eventCount = int(dbutils.widgets.get("EVENT_COUNT"))

assert pcString != "", ": The Primary Connection String must be non-empty"
assert uniqueEHName != "", ": The Unique Event Hubs Name must be non-empty"
assert (eventCount != "") & (eventCount > 0), ": The number of events must be non-empty and greater than 0"

connectionString = pcString.replace(".net/;", ".net/{}/;".format(uniqueEHName))

%python

from azure.eventhub import EventHubClient, Sender, EventData, Offset
from sseclient import SSEClient as EventSource

eh = EventHubClient.from_connection_string(connectionString)

sender = eh.add_sender(partition="0")

eh.run()

%python
wikiChangesURL = 'https://stream.wikimedia.org/v2/stream/recentchange'

for i, event in enumerate(EventSource(wikiChangesURL)):
    if event.event == 'message' and event.data != '':
        sender.send(EventData(add_geo_data(event)))
    if i > eventCount:
        print("OK")
        break

%python
displayHTML("All done!")