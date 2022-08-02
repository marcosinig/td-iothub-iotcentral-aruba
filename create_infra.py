from azure.mgmt.kusto import KustoManagementClient
from azure.mgmt.kusto.models import Cluster, AzureSku
#from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential
from azure.mgmt.kusto.models import ReadWriteDatabase
from datetime import timedelta
from azure.mgmt.kusto.models import Script
import argparse
from azure.mgmt.iothub import IotHubClient
import logging

logger = logging.getLogger(__name__)

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

fileHandler = logging.FileHandler("./{0}.log".format('pythonlog'))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)

def createIotHub(credentials, subscription_id, resource_group_name, location, iot_hub_name, iot_sku, iot_capacity):
    logger.info("create iothub..")
    iothub_client = IotHubClient( credentials, subscription_id)
    # Create iot hub resource
    iot_hub_resource = iothub_client.iot_hub_resource.begin_create_or_update(
        resource_group_name,
        iot_hub_name,
        {
            'location': location,
            'subscriptionid': subscription_id,
            'resourcegroup': resource_group_name,
            'sku': {
                'name': iot_sku,
                'capacity': iot_capacity
            },
            'properties': {
                'enable_file_upload_notifications': False,
                'operations_monitoring_properties': {
                'events': {
                    "C2DCommands": "Error",
                    "DeviceTelemetry": "Error",
                    "DeviceIdentityOperations": "Error",
                    "Connections": "Information"
                }
                },
                "features": "None",
            }
        }
    ).result()
    logger.info("cCreate iot hub resource:\n{}".format(iot_hub_resource))
    shared_access_signature= iothub_client.iot_hub_resource.get_keys_for_key_name(resource_group_name, iot_hub_name, 'iothubowner')
    return (iot_hub_resource.id,shared_access_signature.primary_key)

class DataExplorer:
    class SkuTypes:

        class SkuType:
            no_sla_standard = "Dev_No_SLA_Standard_E2"
            standard_8 = 'Standard_L8'

            def __init__(self, name, tier, capacity) -> None:
                self._name = name
                self._tier = tier
                self._capacity = capacity
        
        def __init__(self) -> None:
            self._region = {
                'australiaeast' : {  self.SkuType.no_sla_standard :  self.SkuType("Dev(No SLA)_Standard_E2a_v4", "Basic", 1),
                                       self.SkuType.standard_8 : self.SkuType("Standard_E8as_v4+1TB_PS", "Standard", 2)
                                    },
                'centralus' : {  self.SkuType.no_sla_standard :  self.SkuType("Dev(No SLA)_Standard_E2a_v4", "Basic", 1),
                                   self.SkuType.standard_8 : self.SkuType("Standard_L8as_v3", "Standard", 2)
                                },
                'uksouth' : {  self.SkuType.no_sla_standard :  self.SkuType("Dev(No SLA)_Standard_E2a_v4", "Basic", 1),
                                   self.SkuType.standard_8 : self.SkuType("Standard_L8s_v3", "Standard", 2)
                                }
            }

        
        def getTier(self, region, type):
            return self._region[region.lower().strip()][type]._tier
        def getCapacity(self, region, type):
             return self._region[region.lower().strip()][type]._capacity
        def getFullName(self, region, type):
             return self._region[region.lower().strip()][type]._name


    def __init__(self, credentials, subscription_id, resource_group_name, location, cluster_name, database_name ):
        self._kusto_management_client = KustoManagementClient(credentials, subscription_id)
        self._resource_group_name = resource_group_name
        self._location = location
        self._cluster_name = cluster_name
        self._database_name = database_name
        self._skuTypes= self.SkuTypes()

    
    def create_cluster(self, sku_name):
        tier = self._skuTypes.getTier(self._location, sku_name)
        capacity = self._skuTypes.getCapacity(self._location, sku_name)
        fullname = self._skuTypes.getFullName(self._location, sku_name)

        logger.info(f"create cluster.. tier: {tier} capacity: {capacity} fullname: {fullname}")
        cluster = Cluster(
            location=self._location, 
            sku=AzureSku(name=fullname, capacity=capacity, tier=tier), 
            enable_streaming_ingest=True)
        poller = self._kusto_management_client.clusters.begin_create_or_update(self._resource_group_name, self._cluster_name, cluster)
        poller.wait()
        

    def create_db(self, soft_delete_period):
        logger.info("create database..")
        #hot_cache_period = timedelta(days=3650)
        self._database_name = self._database_name

        database = ReadWriteDatabase(location=self._location,
                            soft_delete_period=soft_delete_period,
                            #hot_cache_period=hot_cache_period
                            )
        poller = self._kusto_management_client.databases.begin_create_or_update(resource_group_name = self._resource_group_name, cluster_name = self._cluster_name, \
                            database_name = self._database_name, parameters = database)
        poller.wait()
    
    def run_script(self):
        logger.info("add script..")
        script = Script(
            
            script_content=".create table IotUnparsedData(data:dynamic)\n\n\
                .create-or-alter table ['IotUnparsedData'] ingestion json mapping 'iot_unparsed_mapping' '[\
                    '\n'{\"column\":\"data\",\"path\":\"$\"}]'\n\n\
                .alter-merge table IotUnparsedData policy retention softdelete = 7d \n\n\
                .create table TempHumDevice(IotHubDeviceId: string, Timestamp: datetime, Location:string, Temperature:real,Humidity:real)\n\n\
                .create-or-alter  function parseTempHumDevice(){\
                    \nIotUnparsedData\
                    \n| where data.['type'] == '0034'\
                    \n| where isnotempty(data.['objectLastUpdated'])\
                    \n| project IotHubDeviceId=tostring(data['iothub-connection-device-id']), Timestamp=todatetime(data['objectLastUpdated']), Location=tostring(data['location']), Temperature=toreal(data['temperature']), Humidity=toreal(data['humidity'])\
                    \n}\n\n\
                .alter table TempHumDevice policy update \n\
                    @'[{ \"IsEnabled\": true, \"Source\": \"IotUnparsedData\", \"Query\": \"parseTempHumDevice()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": false}]'\n\n\
                .create table MagneticContactDevice(IotHubDeviceId: string, Timestamp: datetime,  Location:string, Contact: bool)\n\n\
                    \n.create-or-alter  function parseMagneticContactDevice(){\
                    \nIotUnparsedData\
                    \n| where isnotempty(data.['objectLastUpdated'])\
                    \n| where data.['type'] == '0033'\
                    \n| project IotHubDeviceId=tostring(data['iothub-connection-device-id']), Timestamp=todatetime(data['objectLastUpdated']), Location=tostring(data['location']),  Contact=tobool(data['contact'])\
                    \n}\n\n\
                .alter table MagneticContactDevice policy update\n\
                     @'[{ \"IsEnabled\": true, \"Source\": \"IotUnparsedData\", \"Query\": \"parseMagneticContactDevice()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": false}]'\n\n\
                .create table SwitchDevice(IotHubDeviceId: string, Timestamp: datetime, Location:string, Button_A0:bool, Button_AI:bool, Button_B0:bool, Button_BI:bool)\n\n\
                .create-or-alter  function parseSwitchDevice(){\
                    \nIotUnparsedData\
                    \n| where isnotempty(data.['objectLastUpdated'])\
                    \n| where data.['type'] == '000B'\
                    \n| project IotHubDeviceId=tostring(data['iothub-connection-device-id']), Timestamp=todatetime(data['objectLastUpdated']), Location=tostring(data['location']), Button_A0=tobool(data['button_A0']),  Button_AI=tobool(data['button_AI']), Button_B0=tobool(data['button_B0']), Button_BI=tobool(data['button_BI'])\
                    \n}\n\n\
                .alter table SwitchDevice policy update\n\
                    @'[{ \"IsEnabled\": true, \"Source\": \"IotUnparsedData\", \"Query\": \"parseSwitchDevice()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": false}]'\n\n\
                .create table MultySensorDevice(IotHubDeviceId: string, Timestamp: datetime, Location:string,Temperature:real,Humidity:real, MagnetContact:bool,  Illumination:int,Acceleration_X:real,Acceleration_Y:real,Acceleration_Z:real,AccelerationStatus:int)\n\n\
                .create-or-alter  function parseMultySensorDevice(){\
                    \nIotUnparsedData\
                    \n| where isnotempty(data.['objectLastUpdated'])\
                    \n| where data.['type'] == '0053'\
                    \n| project IotHubDeviceId=tostring(data['iothub-connection-device-id']), Timestamp=todatetime(data['objectLastUpdated']), Location=tostring(data['location']), Temperature=toreal(data['temperature']), Humidity=toreal(data['humidity']),  MagnetContact=tobool(data['magnetContact']), Illumination=toint(data['illumination']), Acceleration_X=toreal(data['acceleration_X']),  Acceleration_Y=toreal(data['acceleration_Y']),  Acceleration_Z=toreal(data['acceleration_Z']), AccelerationStatus=toint(data['accelerationStatus'])\
                    \n}\n\n\
                .alter table MultySensorDevice policy update\n\
                    @'[{ \"IsEnabled\": true, \"Source\": \"IotUnparsedData\", \"Query\": \"parseMultySensorDevice()\", \"IsTransactional\": false, \"PropagateIngestionProperties\": false}]'\n\n\
                ")           
        poller = self._kusto_management_client.scripts.begin_create_or_update(resource_group_name =  self._resource_group_name, cluster_name = self._cluster_name, \
                        database_name = self._database_name, script_name= 'script1',
                        parameters = script)
        poller.wait()

    def addIotConnection(self, iot_hub_resource_id):
        logger.info("add connection..")
        #KustoDataConnectionsCreateOrUpdate[put]
        BODY = {
            "location":  self._location,
            "kind": "IotHub",
            "iotHubResourceId": iot_hub_resource_id,
            "consumer_group": "$Default",
            "table_name" :  "IotUnparsedData",
            "mappingRuleName": "iot_unparsed_mapping",
            "dataFormat": "JSON",
            "eventSystemProperties": [ "iothub-connection-device-id"],
            "sharedAccessPolicyName": "iothubowner",
            "databaseRouting": "Multi"
        }
        poller = self._kusto_management_client.data_connections.begin_create_or_update(self._resource_group_name,  self._cluster_name, self._database_name,'iotHubConnection', BODY)
        poller.wait()
    
    def add_principal(self, principal_id, email):
        logger.info("add principal..")
        BODY = {
                "value": [
                    {
                    "name": "Marco",
                    "role": "Admin",
                    "type": "User",
                    "fqn": f"aaduser={principal_id}",
                    "email": f"{email}",
                    "app_id": ""
                    }
                ]
        }
        self._kusto_management_client.databases.add_principals(resource_group_name = self._resource_group_name, cluster_name = self._cluster_name, database_name = self._database_name,
                    database_principals_to_add = BODY)

def main():
    # Construct the argument parser
    ap = argparse.ArgumentParser()
    ap.add_argument("-t", "--tenantId", required=True, help="tenant id")
    ap.add_argument("-c", "--clientId", required=True, help="client id")
    ap.add_argument("-cs", "--clientSecret", required=True, help="client secret")
    ap.add_argument("-s", "--subscriptionId", required=True, help="subscription id")
    ap.add_argument("-l", "--location", required=True, help="location")
    ap.add_argument("-des", "--deSku", required=True, help="De Sku Name")
    ap.add_argument("-r", "--resourceGroup", required=True, help="resource group name")
    ap.add_argument("-cl", "--clusterName", required=True, help="cluster name")
    ap.add_argument("-d", "--databaseName", required=True, help="datbase name")
    ap.add_argument("-p", "--principalId", required=True, help="principal Id")
    ap.add_argument("-e", "--email", required=True, help="email")
    ap.add_argument("-iotname", "--iotName", required=True, help="iot name")
    ap.add_argument("-iotsku", "--iotSku", required=True, help="iot sku")
    ap.add_argument("-iotcap", "--iotCapacity", required=True, help="iot capacity")
    ap.add_argument("-deisen", "--deIsEnabled", required=True, help="data Exp Is Enabled")
    ap.add_argument("-desoft", "--deSoftPeriod", required=True, help="data Exp Sof period")
    ap.add_argument("-iotloc", "--iotLocation", required=True, help="iot Location")
    

    args = vars(ap.parse_args())
    logger.info(f"Args is: {args}")

    subscription_id = args['subscriptionId']
    credentials = ClientSecretCredential(
        client_id=args['clientId'],
        client_secret=args['clientSecret'],
        tenant_id=args['tenantId']
    )
    resource_group_name = args['resourceGroup']

    (iot_hub_id, iot_hub_pk) = createIotHub(credentials, subscription_id, resource_group_name, 
                         args['iotLocation'], args['iotName'], args['iotSku'], args['iotCapacity'])
    
    #this file is going to be parsed by the aziotcia script 
    with open('iot_primary_key.txt', 'w') as f:
        print(iot_hub_pk, file=f)

        
    if (args['deIsEnabled'].lower() == 'true'):
        logger.info("DE ENABLED")
        data_explorer = DataExplorer(credentials, subscription_id, resource_group_name, args['location'], args['clusterName'], args['databaseName'])
        data_explorer.create_cluster(args['deSku'])
        data_explorer.create_db( args['deSoftPeriod'])
        data_explorer.run_script()
        data_explorer.addIotConnection(iot_hub_id)
        data_explorer.add_principal(args['principalId'],args['email'])
    else:
        logger.info("DE DISABLED")


if __name__ == '__main__':
    try: 
        main()
    except Exception:
        logger.exception(Exception)