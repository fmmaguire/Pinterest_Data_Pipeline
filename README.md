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

#### File structure
To be added.

#### License information
