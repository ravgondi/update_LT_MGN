import json
import urllib.parse
import boto3
import csv
import io
import os
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.resource('s3')
ec2 = boto3.client('ec2')
mgn = boto3.client('mgn')

print('Loading function')

def lambda_handler(event, context):

    #opens CSV from bucket and extracts fields into dictionary (list_of_json[1:])
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    csv_object = s3.Object(bucket,key)
    csv_file = csv_object.get()['Body'].read().decode('utf-8')
    f = io.StringIO(csv_file)
    reader = csv.DictReader(f, ('ID', 'Status', 'Source Server Name','Target Server Name','Target Server VM Size', 'Target Account', 'Target Availablity Zone','Target VPC', 'Target Subnet','EBS type','Tags','EC2 Instance Profile','Security Groups'))
    list_of_json = [dict(device) for device in reader]
    print (list_of_json)
    f.close()
    #gets list of source server ids and maps to hostname
    lt_hostname_map = map_lt_to_hostname(get_source_server_ids())
    #gets narrowed lists (no repeats, removes unnecessary fields, and changes certain field names)
    lt_narrowed_list = get_lt_narrowed_list(list_of_json[1:])
    combined_map = combined_dict_list(lt_narrowed_list,lt_hostname_map)
    response = modify_lts(combined_map)
    return(response)

#convert bytes
def bytesto(bytes, to, bsize=1024):
    a = {'k' : 1, 'm': 2, 'g' : 3, 't' : 4, 'p' : 5, 'e' : 6 }
    r = float(bytes)
    return bytes / (bsize ** a[to])
 
#union the two dicts where the hostname in each is the same
def combined_dict_list(narrowed_list,lt_hostname_map):
    combined_map_list=[]
    for i in narrowed_list:
        for j in lt_hostname_map:
            combined_map_dict={}
            if i['hostname'] == j['hostname']:
                #union of two dicts
                combined_map_dict = i | j
                combined_map_list.append(combined_map_dict.copy())
    return combined_map_list
 
 
#get MGN source server ids and hostnames
def get_source_server_ids():
    source_server_list=[]
    response = mgn.describe_source_servers(filters={})
    print (response)
    for i in response['items']:
        source_server_dict={}
        source_server_dict['sourceServerID'] =i['sourceServerID']
        source_server_dict['hostname'] = i['sourceProperties']['identificationHints']['hostname'].split('.',1)[0]
        source_server_dict['disks'] = i['dataReplicationInfo']['replicatedDisks']
        source_server_list.append(source_server_dict.copy())
        if i['dataReplicationInfo']['dataReplicationState'] not in ['DISCONNECTED' or 'CUTOVER' or 'DISCOVERED']:
                response = mgn.update_launch_configuration(
                #bootMode='LEGACY_BIOS'|'UEFI',
                #copyPrivateIp=True|False,
                copyTags=True,
                #launchDisposition='STOPPED'|'STARTED',
                #licensing={
                #    'osByol': False
                #    },
                #name='string',
                sourceServerID=i['sourceServerID'],
                targetInstanceTypeRightSizingMethod='NONE'
                )
    return(source_server_list)
def get_lt_narrowed_list(lt_list):
    narrowed_lt_list =[]
    for i in lt_list:
        #tags = i['Tags'].replace(';',',')
        #output = [{"Key": k, "Value": v} for k, v in json.loads(tags).items()]
        temp_dict={}
        temp_dict['Target Server VM Size'] = i['Target Server VM Size']
        temp_dict['Target Subnet'] = i['Target Subnet']
        temp_dict['Target VPC'] = i['Target VPC']
        temp_dict['Tags'] = i['Tags']
        temp_dict['Target Account'] = i['Target Account']
        #temp_dict['EC2 Instance Profile'] = i['EC2 Instance Profile']
        temp_dict['hostname'] = i['Target Server Name'].split('.',1)[0]
        temp_dict['Security Groups'] = i['Security Groups'].replace(';',',').split(",")
        #temp_dict['Tags'] = output
        narrowed_lt_list.append(temp_dict)
    return narrowed_lt_list
   
def map_lt_to_hostname(ss_dict):
    hostname_lt_map_list=[]
    for i in ss_dict:
        hostname_lt_map_dict={}
        response = mgn.get_launch_configuration(sourceServerID=i['sourceServerID'])
        if 'ec2LaunchTemplateID' in response:
            hostname_lt_map_dict['ec2LaunchTemplateID'] = response['ec2LaunchTemplateID']
            hostname_lt_map_dict['hostname'] = i['hostname']
            hostname_lt_map_dict['disks'] = i['disks']
            hostname_lt_map_list.append(hostname_lt_map_dict.copy())

    return hostname_lt_map_list
def modify_lts(lt_list):
 
    for i in lt_list:
        print (i)
        disk_list = []
        for j in i['disks']:
            if 'deviceName' in j:
                disk_dict={}
                disk_dict['DeviceName'] = j['deviceName']
                disk_dict['VirtualName'] = j['deviceName']
                disk_dict['Ebs'] = {}
                #disk_dict['Ebs']['Encrypted'] = True
                disk_dict['Ebs']['VolumeSize']= int(bytesto(j['totalStorageBytes'],'g'))
                disk_dict['Ebs']['DeleteOnTermination'] = True
                disk_dict['Ebs']['VolumeType'] = 'gp3'
                disk_list.append(disk_dict.copy())
 
        response = ec2.create_launch_template_version(
            LaunchTemplateId=i['ec2LaunchTemplateID'],
            VersionDescription='UpdatedByLambdaFunction',
            LaunchTemplateData={
                'EbsOptimized': True,
                'IamInstanceProfile': {
                   'Arn': "arn:aws:iam::" + i['Target Account'] + ":instance-profile/" + i["EC2 Instance Profile"]
                },
                'BlockDeviceMappings': disk_list,
                'NetworkInterfaces': [
                    {
                        'AssociateCarrierIpAddress': False,
                        'AssociatePublicIpAddress': False,
                        'DeleteOnTermination': True,
                        'DeviceIndex': 0,
                        'Groups': i['Security Groups'],
                        'SubnetId': i['Target Subnet'],
                    }
                ],
                #'ImageId': i['AMI ID'],
                'InstanceType': i['Target Server VM Size'],
                'KeyName': key_name,
                'Monitoring': {'Enabled': True},
                'TagSpecifications': [
                    {
                        'ResourceType': 'instance',
                        'Tags': i['Tags']
                       },
                    {
                        'ResourceType': 'volume',
                        'Tags': i['Tags']
 
                    },
                    {
                        'ResourceType': 'network-interface',
                        'Tags': i['Tags']
 
                    }
                ],
 
                'DisableApiTermination': False,
            }
        )
 
 
        response2 = ec2.modify_launch_template(DryRun=False,LaunchTemplateId=i['ec2LaunchTemplateID'],DefaultVersion=str(response['LaunchTemplateVersion']['VersionNumber']))
    return json.dumps(response, default=str)
 

       
