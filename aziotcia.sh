#!/bin/bash

echo $(date) " - ### Starting Script ###"

AZURE_TENANT_ID=$1
AZURE_SUBSCRIPTION_ID=$2
ADMIN_USER=$3
AZURE_CLIENT_ID=$4
AZURE_CLIENT_SECRET=$5
USER_EMAIL=$6
USER_OBJECT_ID=$7
IOT_CENTRAL_NAME=$8
IOT_CENTRAL_LOCATION=$9
IOT_CENTRAL_SKU=${10}
IOT_CENTRAL_SUBDOMAIN=${11}
IOT_CENTRAL_TEMPLATE=${12}
RESOURCE_GROUP_NAME=${13}
DOCKER_HUB_USERNAME=${14}
DOCKER_HUB_PASSWORD=${15}
GIT_TOKEN=${16}
VM_DOMAIN_NAME=${17}
MOBIUS_LICENSE=${18}
IS_IOTHUB_DEPLOY_STR=${19}
IOT_HUB_HOST_NAME=${20}
IOT_HUB_SKU=${21}
DE_CLUSTER_NAME=${22}
DE_DB_NAME=${23}
IOT_HUB_LOCATION=${24}
IOT_CAPACITY=${25}
DE_SOFT_DELETE_PERIOD=${26}
DE_IS_ENABLED=${27}
DE_LOCATION=${28}
DE_SKU=${29}

echo "Script vAruba"
echo "force application id enabled"
IOT_CENTRAL_TEMPLATE=0c4d1694-5018-4328-9f50-2c323d7f072f
IOT_HUB_CONNECTION_STRING=""

echo "IOT_HUB_LOCATION  $IOT_HUB_LOCATION IOT_CAPACITY $IOT_CAPACITY"
echo "DE_SOFT_DELETE_PERIOD  $IOT_HUB_LOCATION DE_IS_ENABLED $IOT_CAPACITY"
echo "DE_LOCATION  $DE_LOCATION DE_DB_NAME $DE_DB_NAME"
                
sudo apt-get -y update 
sudo apt-get -y install ca-certificates curl apt-transport-https lsb-release gnupg 
apt-get install -y software-properties-common > /dev/null
add-apt-repository universe > /dev/null
apt update
sudo apt-get -y install jq
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az extension add --name azure-iot
az login --service-principal -u $AZURE_CLIENT_ID -p $AZURE_CLIENT_SECRET --tenant $AZURE_TENANT_ID

if [ $IS_IOTHUB_DEPLOY_STR == "true" ];then
  echo "Deploy iot Hub"
  sudo apt install python3-pip -y
  pip3 install azure-common azure-mgmt-kusto azure.identity azure-mgmt-iothub 
  wget https://raw.githubusercontent.com/marcosinig/td-iothub-iotcentral-aruba/master/create_infra.py
  python3 create_infra.py -t $AZURE_TENANT_ID -c $AZURE_CLIENT_ID -cs $AZURE_CLIENT_SECRET \
                -s $AZURE_SUBSCRIPTION_ID -l $DE_LOCATION -r $RESOURCE_GROUP_NAME -cl $DE_CLUSTER_NAME -d $DE_DB_NAME \
                -p $USER_OBJECT_ID -e $USER_EMAIL -iotname $IOT_HUB_HOST_NAME  -iotsku $IOT_HUB_SKU -iotcap $IOT_CAPACITY \
                -desoft $DE_SOFT_DELETE_PERIOD -deisen $DE_IS_ENABLED -iotloc $IOT_HUB_LOCATION -des $DE_SKU
  IOT_HUB_PRIMARY_KEY=$(<iot_primary_key.txt)
  IOT_HUB_PRIMARY_KEY_REG_RW=$(<iot_primary_key_registryReadWrite.txt)
  IOT_HUB_CONNECTION_STRING=$(printf "HostName=%s;SharedAccessKeyName=iothubowner;SharedAccessKey=%s" "$IOT_HUB_HOST_NAME" "$IOT_HUB_PRIMARY_KEY")
  IOT_HUB_HOST_NAME_FULL=$(printf "%s.azure-devices.net" "$IOT_HUB_HOST_NAME")
   echo "Deploy DPS"
  DPS_RAND_SUFFIX='' #an contain only alphanumeric
  DPS_NAME=$(printf "DPS%s%s" "$IOT_HUB_HOST_NAME" "$DPS_RAND_SUFFIX")
  DPS_CREATE_ANS=$(az iot dps create --name $DPS_NAME --resource-group $RESOURCE_GROUP_NAME)
  DPS_IDSCOPE=$(echo $DPS_CREATE_ANS | jq '.properties.idScope' |  sed 's/^"\(.*\)".*/\1/')
  DPS_GLOBAL_ENDPOINT=$(echo $DPS_CREATE_ANS | jq '.properties.deviceProvisioningHostName' |  sed 's/^"\(.*\)".*/\1/')
  
  az iot dps linked-hub create --dps-name $DPS_NAME --resource-group $RESOURCE_GROUP_NAME --connection-string $IOT_HUB_CONNECTION_STRING
  DPS_ENROLMENT_PRIMARY_KEY=$(dd if=/dev/urandom bs=56 count=1 status=none | base64)
  DPS_ENROLMENT_SECONDARY_KEY=$(dd if=/dev/urandom bs=56 count=1 status=none | base64)
  az iot dps enrollment-group create -g $RESOURCE_GROUP_NAME --dps-name  $DPS_NAME --enrollment-id 'arubaenrollmentid' --primary-key $DPS_ENROLMENT_PRIMARY_KEY  --secondary-key $DPS_ENROLMENT_SECONDARY_KEY --iot-hubs $IOT_HUB_HOST_NAME_FULL  --allocation-policy hashed
  echo "End iot Hub"
else
  echo "Deploy iot Central"
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  az iot central app create -n $IOT_CENTRAL_NAME -g $RESOURCE_GROUP_NAME -s $IOT_CENTRAL_SUBDOMAIN -l $IOT_CENTRAL_LOCATION -p $IOT_CENTRAL_SKU -t $IOT_CENTRAL_TEMPLATE
  APP_ID=$(az iot central app list -g $RESOURCE_GROUP_NAME | grep application | awk '{print $2}'| sed 's/^"\(.*\)".*/\1/')
  az iot central user create --user-id $USER_OBJECT_ID --app-id $APP_ID --email $USER_EMAIL --role admin
  IOT_OPERATOR_TOKEN=$(az iot central api-token create --token-id adfdasfdsf --app-id $APP_ID --role admin | jq '.token' | sed 's/^"\(.*\)".*/\1/')
fi

echo "Setting up nginix..."
git clone https://$GIT_TOKEN@github.com/marcosinig/td-iaconnect.git
export hostname=$VM_DOMAIN_NAME
cd td-iaconnect; ./setup-https.sh; cd ..;
echo "End nginix"

echo "Setting up your mobiusflow cloud instance..."
echo ""

echo "Running setup-docker"

echo "Installing docker-compose"
curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

echo "Installing Docker"
apt-get install -y docker.io zip > /dev/null
echo "Starting Docker"
systemctl start docker
systemctl enable docker

#docker login --username $DOCKER_HUB_USERNAME --password $DOCKER_HUB_PASSWORD
if [ $IS_IOTHUB_DEPLOY_STR == "true" ];then
echo "Deploy iot Hub Docker"
cat > ~/docker-compose.yml <<EOF
version: '3.8'
volumes:
  mobius-data:
services:
  mobius:
    image: ghcr.io/mobiusflow/mobiusflow-le-tdc2r-aruba-hub:1.10.6-tdc2r-hub-rc.1_1.10.6
    container_name: mobiusflow
    privileged: false
    restart: always
    environment:
      - IS_IOTHUB_DEPLOY=$IS_IOTHUB_DEPLOY_STR
      - IOTHUB_CONNECTIONSTRING=$IOT_HUB_CONNECTION_STRING
      - IOT_APP_NAME=$IOT_CENTRAL_NAME
      - IOT_OPERATOR_TOKEN=$IOT_OPERATOR_TOKEN
      - MOBIUS_LICENCE=$MOBIUS_LICENSE    
      - MOBIUS_ENGINE_API_PORT=9081
      - MOBIUS_ENGINE_API_AUTH_PROVIDER=local
      - MOBIUS_HUB_RESET_PSKS=true
      - MOBIUS_ENABLE_CONFIG_UI=true
      - MOBIUS_HUB_ID=000001
      - MOBIUS_LOCAL_TIMEOUT=10000
      - IOT_HUB_HOSTNAME=$IOT_HUB_HOST_NAME_FULL
      - IOT_HUB_PRIMARY_KEY=$IOT_HUB_PRIMARY_KEY_REG_RW
      - DPS_GLOBAL_ENDPOINT=$DPS_GLOBAL_ENDPOINT
      - DPS_ENROLMENT_PRIMARY_KEY=$DPS_ENROLMENT_PRIMARY_KEY
      - DPS_IDSCOPE=$DPS_IDSCOPE

    ports:
      - 8080:8080
      - 9082:9081
      - 1883:1883
      - 30817:30817
    volumes:
      - mobius-data:/data
    
  tdc2rsetup:
    container_name: tdc2rsetup
    image: ghcr.io/mobiusflow/tdc2r-setup:1.0.0-rc.2
    privileged: false
    restart: always
    ports:
      - 8082:8080
EOF
else
  echo "Deploy iot Central Docker"
cat > ~/docker-compose.yml <<EOF
version: '3.8'
volumes:
  mobius-data:
services:
  mobius:
    image: ghcr.io/mobiusflow/mobiusflow-le-tdc2r:1.10.0-tdc2r-rc.16_1.10.0
    container_name: mobiusflow
    privileged: false
    restart: always
    environment:
      - IS_IOTHUB_DEPLOY=$IS_IOTHUB_DEPLOY_STR
      - IOTHUB_CONNECTIONSTRING=$IOT_HUB_CONNECTION_STRING
      - IOT_APP_NAME=$IOT_CENTRAL_NAME
      - IOT_OPERATOR_TOKEN=$IOT_OPERATOR_TOKEN
      - MOBIUS_LICENCE=$MOBIUS_LICENSE    
      - MOBIUS_ENGINE_API_PORT=9081
      - MOBIUS_ENGINE_API_AUTH_PROVIDER=local
      - MOBIUS_HUB_RESET_PSKS=true
      - MOBIUS_ENABLE_CONFIG_UI=true
      - MOBIUS_HUB_ID=000001
      - MOBIUS_LOCAL_TIMEOUT=10000
    ports:
      - 8080:8080
      - 9082:9081
      - 1883:1883
      - 30817:30817
    volumes:
      - mobius-data:/data
    
  tdc2rsetup:
    container_name: tdc2rsetup
    image: ghcr.io/mobiusflow/tdc2r-setup:1.0.0-rc.2
    privileged: false
    restart: always
    ports:
      - 8082:8080
EOF
fi

rm -rf ~/mobius-cloud-install

echo "DISABLE MOBIOUSFLOW START REMOVE ME"
#echo "Starting mobiusflow"
#cd ~ && docker-compose up &


