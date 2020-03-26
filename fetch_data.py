# import mydb
import json
from database import Database
import csv

dataptr = Database("toor", "iotpltv2", "localhost", "3306")
dataptr.getconnection()

def fetch_devices():
    query = "SELECT id, deviceid, typeid FROM masterdata.devices"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    return mapping
def fetch_deviceTypes():
    query = "SELECT id, typename FROM masterdata.deviceTypes"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
        #print(mapping)
    return mapping


def fetch_simDevices():
    query = "SELECT deviceid, simid FROM masterdata.simDevices"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    return mapping

def fetch_sims():
    query = "SELECT id, simno FROM masterdata.simcards"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    return mapping

def fetch_simInfo():
    query = "SELECT id, data FROM masterdata.simInfo WHERE tag = 'SELF'"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        data = json.loads(eachitem[1])
        mapping[eachitem[0]] = [eachitem[0], data]
    return mapping

def fetch_deviceVehicles():
    query = "SELECT deviceid, vehicleid FROM masterdata.deviceVehicles "
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    return mapping

def fetch_vehicles():
    query = "SELECT id, vehicleno FROM masterdata.vehicles"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    return mapping


def fetch_deviceGroups():
    query = "SELECT deviceid, groupid FROM masterdata.deviceGroups"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        devid = eachitem[0]

        mapping[devid] = mapping.get(devid, list())
        mapping[devid].append(eachitem)

    #print(mapping)
    return mapping

def fetch_groups():
    query = "SELECT id, name, path FROM masterdata.groups"
    qrres = dataptr.executequery(query)
    mapping = dict()
    for eachitem in qrres:
        mapping[eachitem[0]] = eachitem
    #print(mapping)
    return mapping

def getMobno(simid, simInfo):
    if simid in simInfo:
        simData = simInfo[simid][1]
        if 'phoneno' in simData:
            return simData['phoneno']
    return ""

def getSimDetails(devid, simDevices, sims, simInfo):
    #simDevices = { "21": [21, 33], "22": [22, 34] }
    if devid in simDevices:
        simid = simDevices[devid][1]
        phoneno = getMobno(simid, simInfo)
        if simid in sims:
            return sims[simid][1], phoneno
    return "", ""

def getGroupName(gid, groups):
    # print(gid)
    if gid in groups:
        return groups[gid][1]
    return ""
def getDeviceType(devid,deviceTypes,devices):
    if devid in devices:
        typeid = devices[devid][2]
        if typeid in deviceTypes:
            return (deviceTypes[typeid][1])
            


def getGroupPathNames(devid, deviceGroups, groups):
    path_name_list = list()

    if devid in deviceGroups:
        d_id = deviceGroups[devid]
        # if len(d_id) > 1:
        for insideid in d_id:
            #print(insideid)
            gid = insideid[1]
            #print(gid)
            split_paths = groups[gid][2].split('/')[1:-1]
            
            path_name_sub_list = list();
            for sgid in split_paths:
                sgid = int(sgid)
                gname = getGroupName(sgid, groups)
                path_name_sub_list.append(gname)
            path_name_list.append('/'.join(path_name_sub_list))
    return path_name_list
    return ""


def getVehicleNo(devid, deviceVehicles, vehicles):
    if devid in deviceVehicles:
        vehid = deviceVehicles[devid][1]
        if vehid in vehicles:
            return vehicles[vehid][1]
    return ""

def start():
    devices = fetch_devices()
    simDevices = fetch_simDevices()
    sims = fetch_sims()
    simInfo = fetch_simInfo()
    deviceVehicles = fetch_deviceVehicles()
    vehicles = fetch_vehicles()
    deviceGroups = fetch_deviceGroups()
    groups = fetch_groups()
    deviceTypes = fetch_deviceTypes()

    outfile = csv.writer(open('masterdatafeb26.csv', 'w'))
    outfile.writerow(['Device No', 'Device Type', 'Sim No','Phone Number', 'Vehicle Number', 'Group'])

    for gid in groups:
       # print(type(gid), gid)
        break

    for devid in devices:
        devno = devices[devid][1]
        simno, phoneno = getSimDetails(devid, simDevices, sims, simInfo)
        vehicleno = getVehicleNo(devid, deviceVehicles, vehicles)
        # group = getGroup(devid,deviceGroups,groups)
        gnames = getGroupPathNames(devid, deviceGroups, groups)
        devicetypes = getDeviceType(devid,deviceTypes,devices)
        outfile.writerow([devno,devicetypes, simno, phoneno, vehicleno, ','.join(gnames)])
        #print(devno, simno, phoneno, vehicleno, '/'.join(gnames)) 

if __name__ == '__main__':
    start()
    dataptr.closedatabaseconnection()

# fetch_device()  
# fetch_simid()  
#print(deviceid)
#print(devicemap)