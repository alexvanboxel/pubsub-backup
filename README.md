This code sample has been updated for Apache Beam 2.3+


```
java -jar build/libs/pubsub-backup-backup-1.0.jar \
 --project=my-project \
 --zone=europe-west1-d \
 --streaming \
 --stagingLocation=gs://my-bucket/tmp/beam/ \
 --runner=DataflowRunner \
 --workerMachineType=n1-standard-4
```


```json
[
  {
      "name": "topic", 
      "type": "string", "mode": "required",
      "description": "Topic name where the message was published"
  },
  {
      "name": "payload", 
      "type": "bytes", "mode": "required",
      "description": "Message binary payload"
  },
  {
      "name": "attributes", 
      "type": "string", "mode": "nullable",
      "description": "Optional attributes of the message"
  },
  {
      "name": "msg_id", 
      "type": "string", "mode": "nullable",
      "description": "Id of the message"
  },
  {
      "name": "msg_timestamp", 
      "type": "timestamp", "mode": "required",
      "description": "Timestamp that the message was published"
  },
  {
      "name": "process_timestamp", 
      "type": "timestamp", "mode": "required",
      "description": "Timestamp that the backup entry was processed by the pipeline"
  },
  {
      "name": "errors", 
      "type": "string", "mode": "nullable",
      "description": "Errors encountered during processing"
  }
]
```
