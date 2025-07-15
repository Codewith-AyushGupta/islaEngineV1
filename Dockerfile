# Use the AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12-arm64


# Set working directory
ENV HOME=/tmp
WORKDIR /var/task

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY . .

# Set the Lambda handler
# CMD ["AsyncDuckDb.lambda_handler","SyncDuckDb.lambda_handler"]