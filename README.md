# Finance Data to AWS S3 Uploader in Python

### 1. Environment
- Cloud Provider: Amazon Web Service (AWS)
- Platform: AWS Lambda
- Language: Python 3.9

### 2. Dependencies:
- Boto3
- Pandas
- Numpy
- finance-datareader
- beautifulsoup4

### 3. Purpose:
- This Script gets finance data from Investing.com and upload it to AWS S3. If it is exist, it will be updated.
### 4. Elements and Explanation:
Python Library Boto3 allows the lambda to get and upload and update the CSV file from S3.
### 5. Parameters:
  - AWS Account Access Key ID
  - AWS Account Secret Key ID
  - Bucket Name
  - CSV File Path

### 6. Deployment Process:
  - Make a package containing all the dependencies and the given python script.
  - Deploy the package on lambda
  - Execute
