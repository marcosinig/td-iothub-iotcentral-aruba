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
            def __init__(self, tier, capacity) -> None:
                self.tier = tier
                self.capacity = capacity
        def __init__(self) -> None:
            self._skus ={}
            self._skus["Standard_L8as_v3"] = self.SkuType( "Standard", 2)
            self._skus["Dev(No SLA)_Standard_E2a_v4"] = self.SkuType( "Basic", 1)
        def getTier(self, name):
            return self._skus[name].tier
        def getCapacity(self,name):
             return self._skus[name].capacity


    def __init__(self, credentials, subscription_id, resource_group_name, location, cluster_name, database_name ):
        self._kusto_management_client = KustoManagementClient(credentials, subscription_id)
        self._resource_group_name = resource_group_name
        self._location = location
        self._cluster_name = cluster_name
        self._database_name = database_name
        self._skuTypes= self.SkuTypes()

    
    def create_cluster(self, sku_name):
        logger.info(f"create cluster.. tier: {self._skuTypes.getTier(sku_name)} capacity: {self._skuTypes.getCapacity(sku_name)}")
        cluster = Cluster(location=self._location, sku=AzureSku(name=sku_name, capacity=self._skuTypes.getCapacity(sku_name), 
                            tier=self._skuTypes.getTier(sku_name)), enable_streaming_ingest=True)
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
            script_content=".create-merge table iot_parsed (IotHubDeviceId:string,Timestamp:datetime,Temperature:real,Humidity:real,Contact:bool,MagnetContact:bool,\
                Illumination:int,Acceleration_X:real,Acceleration_Y:real,Acceleration_Z:real,AccelerationStatus:int,Button_A0:bool,Button_AI:bool,Mutton_B0:bool,Button_BI:bool,\
                Button_B0:bool, Type:string)\n\n\
                .alter table iot_parsed policy streamingingestion enable\n\n\
                 .create-or-alter table ['iot_parsed'] ingestion json mapping 'iot_parsed_mapping' '[\
                '\n'{\"column\":\"Timestamp\",\"path\":\"$.objectLastUpdated\",\"datatype\":\"datetime\"},\
                '\n'{\"column\":\"IotHubDeviceId\",\"path\":\"$.iothub-connection-device-id\",\"datatype\":\"string\"},\
                '\n'{\"column\":\"Type\",\"path\":\"$.type\",\"datatype\":\"string\"},\
                '\n'{\"column\":\"Contact\",\"path\":\"$.contact\",\"datatype\":\"bool\"},\
                '\n'{\"column\":\"MagnetContact\",\"path\":\"$.magnetContact\",\"datatype\":\"bool\"},\
                '\n'{\"column\":\"Humidity\",\"path\":\"$.humidity\",\"datatype\":\"real\"},\
                '\n'{\"column\":\"Temperature\",\"path\":\"$.temperature\",\"datatype\":\"real\"},\
                '\n'{\"column\":\"Illumination\",\"path\":\"$.illumination\",\"datatype\":\"int\"},\
                '\n'{\"column\":\"Acceleration_X\",\"path\":\"$.acceleration_X\",\"datatype\":\"real\"},\
                '\n'{\"column\":\"Acceleration_Y\",\"path\":\"$.acceleration_Y\",\"datatype\":\"real\"},\
                '\n'{\"column\":\"Acceleration_Z\",\"path\":\"$.acceleration_Z\",\"datatype\":\"real\"},\
                '\n'{\"column\":\"AccelerationStatus\",\"path\":\"$.accelerationStatus\",\"datatype\":\"int\"},\
                '\n'{\"column\":\"Button_A0\",\"path\":\"$.button_A0\",\"datatype\":\"bool\"},\
                '\n'{\"column\":\"Button_AI\",\"path\":\"$.button_AI\",\"datatype\":\"bool\"},\
                '\n'{\"column\":\"Button_B0\",\"path\":\"$.button_B0\",\"datatype\":\"bool\"},\
                '\n'{\"column\":\"Button_BI\",\"path\":\"$.button_BI\",\"datatype\":\"bool\"}\
                ]'" )
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
            "table_name" :  "iot_parsed",
            "mappingRuleName": "iot_parsed_mapping",
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