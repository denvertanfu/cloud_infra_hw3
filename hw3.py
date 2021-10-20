# pip install boto3
import boto3
bucket_name = 'datacont-name-1019'
access_key_id = ''
access_key = ''
s3 = boto3.resource('s3',
                    aws_access_key_id=f'{access_key_id}',
                    aws_secret_access_key=f'{access_key}')
try:
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'}) 
except Exception as e:
    print("exception")
    print(e)

bucket = s3.Bucket(bucket_name)
bucket.Acl().put(ACL='public-read')


def create_dynamoDB_table():
	dyndb = boto3.resource('dynamodb',
							region_name='us-west-2',
							aws_access_key_id=f'{access_key_id}', aws_secret_access_key=f'{access_key}'
							)
	try:
		table = dyndb.create_table(
		TableName='DataTable', KeySchema=[
		            {
		                'AttributeName': 'PartitionKey',
		                'KeyType': 'HASH'
		}, 
		{
						'AttributeName': 'RowKey',
		                'KeyType': 'RANGE'
		            }
		], AttributeDefinitions=[
		            {
		                'AttributeName': 'PartitionKey',
		                'AttributeType': 'S'
		}, {
						'AttributeName': 'RowKey',
		                'AttributeType': 'S'
		            },
		], ProvisionedThroughput={
		            'ReadCapacityUnits': 5,
		            'WriteCapacityUnits': 5
		        }
		)
	except Exception as e:
		print(e)
		#if there is an exception, the table may already exist.
		table = dyndb.Table("DataTable")
	table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
	print(table.item_count)

# create_dynamoDB_table()

def reading_csv_file():
	import csv
	with open('experiments.csv', 'r') as csvfile: 
		csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
		next(csvf)
		for item in csvf:
			print(item)
			print(item[0], item[1], item[2], item[3], item[4])
			body = open('/Users/tan/Desktop/CMU/2021_Fall/Cloud_Infrastructure/HW3/'+item[4], 'rb') 
			s3.Object(f'{bucket_name}', item[4]).put(Body=body)
			md = s3.Object(f'{bucket_name}', item[4]).Acl().put(ACL='public-read')
			url = rf"https://s3-us-west-2.amazonaws.com/{bucket_name}/"+item[4] 
			# metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
			# 		'description' : item[4], 'date' : item[2], 'url':url}
			metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
					'conductivity' : item[2], 'concentration' : item[3], 'url':url}
			try: 
				dyndb = boto3.resource('dynamodb',
							region_name='us-west-2',
							aws_access_key_id=f'{access_key_id}', aws_secret_access_key=f'{access_key}'
							)
				table = dyndb.Table("DataTable")
				table.put_item(Item=metadata_item)

			except Exception as e:
				print("item may already be there or another failure")
				print(e)

# reading_csv_file()

def search_for_item():
	dyndb = boto3.resource('dynamodb',
							region_name='us-west-2',
							aws_access_key_id=f'{access_key_id}', aws_secret_access_key=f'{access_key}'
							)
	table = dyndb.Table("DataTable")

	response = table.get_item(Key={
									'PartitionKey': '1',
									'RowKey': '-1',
    								}
								)
	item = response['Item']
	print(item)
	response = table.get_item(Key={
									'PartitionKey': '2',
									'RowKey': '-2',
    								}
								)
	item = response['Item']
	print(item)
	response = table.get_item(Key={
									'PartitionKey': '3',
									'RowKey': '-2.93',
    								}
								)
	item = response['Item']
	print(item)
	# print(response)

search_for_item()



# create_s3_object()
# upload_file_to_bucket()
# create_dynamoDB_table()
# reading_csv_file()
# search_for_item()
