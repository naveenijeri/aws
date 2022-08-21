from urllib import response
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
import time

client = boto3.client('dynamodb')

#create DynamoDB table
def create_movie_table():
    table = client.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH' #Partition Key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE' #Sort Key         
            }
        ],
        AttributeDefinitions = [
            {
                'AttributeName': 'year',
                'AttributeType': 'N'

            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S',

            }
        ],

        ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
       }
    )
    return table

#create record in a DynamoDB table
def put_movie(title, year, plot, rating):
    response = client.put_item(
        TableName='Movies',
        Item={
            'year':{
                'N': "{}".format(year),
            },
            'title':{
                'S': "{}".format(title),
            },
            'plot':{
                'S': "{}".format(plot),
            },
            'rating':{
                "N": "{}".format(rating)
            }
        }
    )
    return response

#Get a record from DynamoDb table
def get_movie(title, year):
    try:
        response = client.get_item(
            TableName='Movies',
            Key={
                'year':{
                    'N': "{}".format(year)
                },
                'title':{
                    'S': "{}".format(title)
                }
            }
        )
    except Exception as e:
        print(e)
    else:
       return response["Item"]

#Update a record in DynamoDB table
def update_movie(title, year, rating, plot, actors):
    response = client.update_item(
        TableName='Movies',
        Key={
            'year':{
                'N': '{}'.format(year)
            },
            'title':{
                'S': '{}'.format(title)
            }
        },
        ExpressionAttributeNames={
            '#R': 'rating',
            "#P": 'plot',
            "#A": 'actors'
        },
        ExpressionAttributeValues={
            ':r': {
                'N': '{}'.format(rating)
            },
            ':p': {
                'S': '{}'.format(plot)
            },
            ':a': {
                'S': '{}'.format(actors)
            },

        },
        UpdateExpression= 'SET #R = :r, #P = :p, #A = :a',
        ReturnValues="UPDATED_NEW"
    )
    return response

#Delete an Item in DynamoDB table
def delete_underrated_movie(title, year, rating):
    try:
        response = client.delete_item(
            TableName='Movies',
            Key={
                'year': {
                    'N': '{}'.format(year),  
                },
                'title':{
                    'S': '{}'.format(title),
                }
            },
            ConditionExpression="rating <= :a",
            ExpressionAttributeValues= {
                ':a':{
                    'N': '{}'.format(rating)
                }
            }
            
        )
    except Exception as e:
        print(e)
    else:
        return response
if __name__ == '__main__':
    #Create DynamoDB
    movie_table = create_movie_table()
    print("Create DynamoDB successeded")
    print("Table Status:{}".format(movie_table))

    #Insert into DynamoDB
    movie_resp = put_movie("KGF Movie", 2022, "Nothing happens at all.", 5)
    print("Insert into DynamoDB successeded")

    #Get an item from DynamoDB
    movie = get_movie("KGF Movie", 2022)
    print(movie)
    
    #Update an item in DynamoDB
    update_response = update_movie("KGF Movie", 2022, 5.5, "Everything happens all at once.", ["Larry", "More", "Curly"])
    print("update and item in DynamoDB successeded")

    #Delete an item in DynamoDB Table
    delete_response = delete_underrated_movie("KGF Movie", 2022, 5.5)
    if delete_response:
        print("delete an item in DynamoDB Table")