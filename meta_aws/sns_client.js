import {config} from "../config.js";
import {CreateTopicCommand, SubscribeCommand,PublishCommand } from "@aws-sdk/client-sns";

import  { SNSClient } from "@aws-sdk/client-sns";
import { resourceLimits } from "worker_threads";
import { getArnResources } from "@aws-sdk/middleware-bucket-endpoint";
// Set the AWS Region.
const REGION = config.REGION; //e.g. "us-east-1"
// Create SNS service object.
console.log(config.REGION)
const snsClient = new SNSClient({ region: config.REGION });


const createTopic = async (
    name,
    topic,
    Attributes=undefined,
) => {
    let attributes =  {"FifoTopic": "true",
    "ContentBasedDeduplication": "true"}
    if (Attributes !== undefined) {
        Attributes.forEach((key,val) => {
            attributes[key] = val
        });
    }
    let tparams = {
         Name: name, //config.TOPIC_NAME,
         Attributes:attributes,
         };

    const data = await snsClient.send(new CreateTopicCommand(tparams));
    topic.arn =  data.TopicArn
};


const subscribe = async (
    TopicArn,
    queue_arn,
    result,
    Attributes=undefined,
) => {

    let attributes = ["All"];
    if (Attributes !== undefined){
        for (var attribute of Attributes) {
            attributes.push(attribute)
        }
    }
 
   // Set the parameters
    const params = {
        Protocol: "sqs" /* required */,
        TopicArn: TopicArn, //TOPIC_ARN
        Endpoint: queue_arn, //queue_arn
  };
    var data = await snsClient.send(new SubscribeCommand(params))
    result.data = data

}

const publishMessage = async (
    msg,
    TopicArn,
    topicname,
    result,
) => {

    let params = {
        TopicArn: TopicArn,
        Message: msg,
        MessageGroupId: topicname,
    }
    var res = await snsClient.send(new PublishCommand(params))
    result.data = res
}
export  { snsClient, createTopic ,subscribe,publishMessage};