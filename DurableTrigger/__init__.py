# This function an HTTP starter function for Durable Functions.
# Before running this sample, please:
# - create a Durable orchestration function
# - create a Durable activity function (default name is "Hello")
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt
 
import logging
import time
from azure.durable_functions import DurableOrchestrationClient
import azure.functions as func


async def main(mytimer: func.TimerRequest, starter: str, message):
     
    logging.info(starter)
    client = DurableOrchestrationClient(starter)
    instance_id = await client.start_new("DurableOrchestration")
    response = logging.info(f"Started orchestration with ID = '{instance_id}'.")
    message.set(response)