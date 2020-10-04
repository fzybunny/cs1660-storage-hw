#! /usr/bin/env python3

import csv
import boto3
import random
import string

# Add key id and secret key here
key_id = ''
secret_key = ''

def get_s3_obj():
	return boto3.resource('s3', region_name='us-west-2',
		aws_access_key_id=key_id,
		aws_secret_access_key=secret_key
	)

def create_bucket(s3):
	while True:
		try:
			bucket_name = ''.join(
				random.choice(string.ascii_lowercase) for _ in range(5))
			bucket_name = 'bucket-' + bucket_name
			print(bucket_name)
			s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
				'LocationConstraint': 'us-west-2'})
			break

		except:
			print('Failed to create bucket. Trying a new name')

	return bucket_name

def create_table():
	dyndb = boto3.resource('dynamodb', region_name='us-west-2',
		aws_access_key_id=key_id,
		aws_secret_access_key=secret_key
	)

	try:
		table = dyndb.create_table(
			TableName='DataTable',
			KeySchema=[
				{ 'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
				{ 'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
			],
			AttributeDefinitions=[
				{ 'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
				{ 'AttributeName': 'RowKey', 'AttributeType': 'S'}
			],
			ProvisionedThroughput={
				'ReadCapacityUnits': 5,
				'WriteCapacityUnits': 5
			}
		)
	except:
		table = dyndb.Table('DataTable')

	table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')

	return table


def upload_data(s3, table, bucket_name):
	url_base = 'https://s3-us-west-2.amazonaws.com/' + bucket_name

	with open('./experiments.csv', 'r') as csvfile:
		reader = csv.reader(csvfile, delimiter=',', quotechar='|')
		for item in reader:
			body = open('./datafiles/'+item[3], 'rb')
			s3.Object(bucket_name, item[3]).put(Body=body)
			md = s3.Object(bucket_name, item[3]).Acl().put(ACL='public-read')

			url = url_base + '/' +item[3]
			metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
				'description': item[4], 'date': item[2], 'url':url}

			try:
				table.put_item(Item=metadata_item)
			except:
				print('Failed to put item' + item)


def search(table):
	response = table.get_item(
		Key={
			'PartitionKey': 'experiment3',
			'RowKey': '4'
		}
	)
	item = response['Item']
	print(str(item) + '\n\n' + str(response))



def main():
	s3 = get_s3_obj()
	bucket_name = create_bucket(s3)
	table = create_table()
	upload_data(s3, table, bucket_name)
	search(table)

if __name__ == '__main__':
	main()
