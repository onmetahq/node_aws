import {createTopic,subscribe,publishMessage} from "./sns_client.js"
import {createQueue,getQueueAttributes,receiveMessage,getQURL,deletemessage} from "./sqs_client.js"
import { exit } from "process";

var arns = {} //save topic,queue arns for later usage

const generateResourcebyQname = function(queuename,arn){
    var resource = arn.split(":")
    resource[2] = "sqs"
    resource.splice(-1)
    resource.push(queuename)
    resource = resource.join(":")
    return resource
}

const createQandtopic = async (
    topicname,
    queuename,
    result,
    Attributes=undefined,
) => {
    if (arns!=undefined || arns !={})
        if(Object.keys(arns).includes(queuename)) {
        console.log("Queue already exists: => ",queuename)
        result.queueURL = arns[queuename]
    } else{
        var res ={}
        console.log("Creating Topic: => ",topicname)
        await createTopic(topicname,res)
        console.log("Topic created: => ",res.arn)
        arns[topicname] = res.arn

        let Condition = {
            "ArnEquals": {
              "aws:SourceArn": res.arn
            }
          };
          let Statement = {
            "Sid": "id_",
            "Effect": "Allow",
            "Principal": {
              "Service": "sns.amazonaws.com"
            },
            "Action": "SQS:SendMessage",
            "Resource": generateResourcebyQname(queuename,res.arn),
            "Condition": Condition,
          };
          let policy = {
            "Version": "2008-10-17",
            "Id": "SQSDefaultPolicy",
            "Statement": [
              Statement
            ]
          }
        var qurl = {}
        try{
          var res ={}
          console.log("Creating Queue: =>",queuename)
          await createQueue(queuename,policy,res)
          console.log("Queue created: =>",res.data)
          qurl = res.data
        }  catch(err){
          if(err.name == "QueueNameExists"){
            console.log("QueueNameExists ",generateResourcebyQname(queuename))
            await getQURL(queuename,qurl)
            qurl = qurl.url
          }else{
            console.log("Error creating queue: =>",err)
            exit(-1)
          }
        }
    
        //fetch queue_arn to subscribe
        var qres={}
        await getQueueAttributes(qurl,qres)
        arns[queuename] = qres.data.QueueArn

        var sres={}
        await subscribe(arns[topicname],arns[queuename],sres)
        console.log("Subscribed: =>",sres.data.SubscriptionArn)
    }
};

const sendMessage = async(topicname,message,result)=>{
    var res = {}
    console.log("Sending message to: =>",arns[topicname])
    await publishMessage(message,arns[topicname],topicname,res)
    console.log("Message sent status: =>",res.data.$metadata.httpStatusCode)
    result.data = res.data
}


const getMessage = async(queuename,result) => {
    var res = {}
    console.log("Get message from : =>",arns[queuename])
    await getQURL(queuename,res)
    //console.log("get url ",res.url)
    var rres = {}
    await receiveMessage(res.url,rres)
    console.log("Got message: =>",rres.data.$metadata.httpStatusCode)
    result.Messages = rres.data.Messages

}

const deleteMessage = async(queuename,receiptHandle,stats) => {
  var data = {}
  var res = {}
  await getQURL(queuename,data)
  await deletemessage(data.url,receiptHandle,res)
  console.log("delete: => ",res.data.$metadata.httpStatusCode)
  stats.statuscode = res.data['$metadata']['httpStatusCode']
}

export {createQandtopic,arns,sendMessage,getMessage,deleteMessage}