import  { SQSClient,SQS ,CreateQueueCommand,GetQueueAttributesCommand, ReceiveMessageCommand,GetQueueUrlCommand, DeleteMessageCommand} from "@aws-sdk/client-sqs";
import  { SNSClient } from "@aws-sdk/client-sns";

import {config} from "../config.js";
import { get } from "http";

// Set the AWS Region.
const REGION = config.REGION; //e.g. "us-east-1"
// Create SQS service object.
const sqsClient = new SQSClient({ region: REGION });


const createQueue = async (
    name,
    policy,
    result,
    Attributes=undefined,
) => {
    
    let attributes = {
        DelaySeconds: "0", // Number of seconds delay.
        MessageRetentionPeriod: "86400", // Number of seconds delay.
        FifoQueue:true,
        ContentBasedDeduplication:true,
        FifoThroughputLimit:"perMessageGroupId",
        DeduplicationScope:"messageGroup",
        SqsManagedSseEnabled:false,
        Policy: JSON.stringify(policy),
      }
      if (Attributes !== undefined) {
        Attributes.forEach((key,val) => {
            attributes[key] = val
        });
      }
    const params = {
        QueueName: name, //SQS_QUEUE_URL 
        Attributes: attributes,
        };

    const data = await sqsClient.send(new CreateQueueCommand(params));
    result.data = data.QueueUrl
};

const getQURL = async(queuename,result)=>{
  let params = {
    QueueName:queuename
  }
  const data = await sqsClient.send(new GetQueueUrlCommand(params));
  result.url = data.QueueUrl
}
const getQueueAttributes = async (
    queue_url,
    result,
    Attributes=undefined,
) => {
    let attributes = ["All"];
    if (Attributes !== undefined){
        for (var attribute of Attributes) {
            attributes.push(attribute)
        }
    }
    let params = {
        QueueUrl: queue_url,
        AttributeNames: ['All'],
    }
    var res = await sqsClient.send(new GetQueueAttributesCommand(params))
    result.data = res.Attributes
}

const receiveMessage = async(
  queue_url,
  result,
)=>{
  let params = {
    QueueUrl: queue_url,
    AttributeNames: ['All'],
    WaitTimeSeconds:20,
    MaxNumberOfMessages:10,
}
  var res = await sqsClient.send(new ReceiveMessageCommand(params))
  //console.log("sq receive",res.Messages)
  result.data = res
}

const deletemessage= async(
  queue_url,
  receiptHandle,
  result,
)=>
{
  let params = {
    QueueUrl:queue_url,
    ReceiptHandle:receiptHandle,
  }
  var res = await sqsClient.send(new DeleteMessageCommand(params))
  result.data = res
}
export  { 
    sqsClient,
    createQueue,
    getQueueAttributes,
    receiveMessage,
    getQURL,
    deletemessage,
 };

