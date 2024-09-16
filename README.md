# Pinterest_Data_Pipeline
Pinterest crunches billions of data points every day to provide more value to their users. This project creates a similar system using the AWS Cloud.
You will require an AWS account and access to dummy data to run this code.
This repo should serve as a guide on how to set up AWS and operate Kafka data processing for both batch processing and stream processing.

<!-- TABLE OF CONTENTS -->
<details> 
  <summary>Table of Contents</summary> 
  <ol> 
    <li> 
      <ul> 
        <li><a href="#installation">Installation</a></li> 
        <li><a href="#usage-batch-processing">Usage Batch Processing</a></li> 
        <li><a href="#file-structure">File Structure</a></li> 
        <li><a href="#license-information">License Information</a></li> 
      </ul> 
    </li> 
  </ol> 
</details> 

#### Installation
Download dummy data: [Pinterest data](https://aicore-portal-public-prod-307050600709.s3.eu-west-1.amazonaws.com/project-files/eec4e4d1-56ca-4ce9-aa4b-bedb3c84f31f/user_posting_emulation.py)\
Download kafka: [Kafka package](https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz)
Download IAM MSK authentication package: [IAM MSK authentication package](https://github.com/aws/aws-msk-iam-auth)
Download Confluent Kafka REST Proxy package: [Confluent package](https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz)
Import random package: `import random`

#### Usage Batch Processing
Configure the Amazon EC2 instance to use as an apache Kafka client
- Create a .pem key file locally
  - Navigate to the Parameter Store in your AWS account
  - Idenitfy your EC2 instance and under the Value field select Show
  - Copy its entire value (including BEGIN and END) and paste to the local .pem file
- Connect to the EC2 instance
  - Follow the Connect instructions (SSH client) on the EC2 console to do this
- Set up Kafka on the EC2 instance
  - Create an IAM authenticated MSK cluster
  - Install Kafka on your client EC2 machine, set up security rules for the instance as required. Make sure to install the same version of Kafka as the one your cluster is running on.
  - Install IAM MSK authentication package on your EC2 machine
  - Copy the ARN of your IAM role and use it on the Add a principal with Edit trust policy of the Trust relationships tab
  - Configure Kafka client to use AWS IAM authentication by editing the client.properties file within kafka_"version"/bin
- Create Kafka topics
  - Naviagte to the MSK cluster and copy the Bootstrap servers string and the Plaintext Apache Zookeeper connection string
  - Create a .pin topic for Pinterest posts data
  - Create a .geo topic for posts geolocation data
  - Create a .user topic for posts user data
  - Create topic Kafka command: `./kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <topic_name>`

Connect MSK cluster to S3 bucket
- Create a custoom plugin with MSK Connect
  -  Install Confluent.io AMazon S3 Connector and copy it to your S3 bucket
  -  Create a custom plugin in the MSK Connect console
- Create a connector with MSK Connect
  - Create and configure a connector, update topics.regex and bucket name
  - Select the IAM role used for authentication to the MSK cluster in the Access permissions tab
 
Configure API in API Gateway
- Build a Kafka REST proxy integration method for the API
  - Create a resource that allows you to build a PROXY integration for your API
  - Create an HTTP ANY method for this resource using the PublicDNS of your EC2 machine for the Endpoint URL.
  - Deploy the API and save the Invoke URL
- Set up the Kafka REST proxy on the EC2 client
  - Modify the kafka-rest.properties file with the IAM authentication needed for the MSK cluster
  - Start the REST proxy on the EC2 client machine
- Send data to the API
  - Modify the user_posting_emulation.py as required to send data to the Kadka topics using the API Invoke URL

Access data in Databricks
- Mount S3 buket to Databricks
  - Create dataframes for each dataset
  - Read in the data from JSONs and save them as tables in databricks

Data cleaning with Spark
- Clean the Dataframes
- Find the most popular category in each country
- Find which was the mos tpopular category in each year
- Find the user with the most followers in each country
- Find the most popular category for different age groups
- Find the median follower counnt for different age groups
- Fund how many users have joined each year
- Find the median follower count of users based on their joining year
- Find the medina follower count of users based on their joining year and age group 

AWS Managed Workflows for Apache Airflow
- Create and upload a DAG to a MWAA environment
  - Create MWAA environment, S3 bucket, API token in Databricks and requirements.txt as needed
- Trigger the DAG to confirm it runs   

#### Usage Stream Processing
AWS Kinesis
- Create data streams for each of the Pinterest data tables
- Configure API with Kinesis proxy connection so that it can
 - List streams in Kinesis
 - Create, describe and delete streams in Kinesis
 - Add records to streams in Kinesis
- Use user_posting_emulation_stream.py to send data to the Kinesis streams
- Read the data in a Databricks workbook from Kinesis
- Clean the streaming data
- Write the streaming data to Delta tables

#### File structure
- user_posting_emulation.py
  - Batch processing data pipeline
- user_posting_emulation_stream.py
  - Stream processing data pipeline
- Mount S3 bucket.ipynb
  - Batch processing data cleaning
- kinesis_streams_notebook.ipynb
  - Stream processing data cleaning
- dag.py
  - Dag template
- CloudPinterestPipeline.png
  - Architecture diagram

#### License information
