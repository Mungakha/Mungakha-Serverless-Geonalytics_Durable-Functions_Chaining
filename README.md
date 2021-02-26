# Introduction 
This sample demonstrates how to go about implementing the  [Function Chaining](https://docs.microsoft.com/en-us/azure/azure-functions/durable/durable-functions-overview?tabs=csharp#chaining) pattern in Python Durable Functions. It uses a [timerTrigger](https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-timer?tabs=csharp)

It additionally demonstrates how to go about setting intermittent status while an orchestation is executing. This enables a user to monitor the status of the orchestration through a custom message set by the user.
# Getting Started
Create a **local.settings.json** file with your **<your connection string>**

```
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "<your connection string>",
    "FUNCTIONS_WORKER_RUNTIME": "python"
  }
}
```

# Getting Started

# Run Code
 Once initialized in VScode, press **F5** and wait for it to execute

# Contribute
