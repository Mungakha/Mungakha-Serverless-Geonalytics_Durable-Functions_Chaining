# This function is not intended to be invoked directly. Instead it will be
# triggered by an HTTP starter function.
# Before running this sample, please:
# - create a Durable activity function (default name is "Hello")
# - create a Durable HTTP starter function
# - add azure-functions-durable to requirements.txt
# - run pip install -r requirements.txt

import logging
import json

import azure.functions as func
import azure.durable_functions as df


def orchestrator_function(context: df.DurableOrchestrationContext):
    # Chained functions - output of a function is passed as
    # input to the next function in the chain
    r1 = yield context.call_activity("DurableActivityNF", 0)
    context.set_custom_status(f'{r1} ->')
    r2 = yield context.call_activity("DurableActivityPTN", r1)
    context.set_custom_status(f'{r1} -> {r2} ->')
    r3 = yield context.call_activity("DurableActivityTECH1", r2)
    context.set_custom_status(f'{r1} -> {r2} -> {r3}')
    return r3

main = df.Orchestrator.create(orchestrator_function)