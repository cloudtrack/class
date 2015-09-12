//Lambda Function listens to Kinesis Stream "Latest" BatchSize:1 event, 
//and posts to dynamodb one record at a time

//processes following kinesis data record JSON 
//{"device_id":"jin1234","time":20150422220852,"device":"edison","sensors":[{"telemetryData": {"xval":-0.229598,"yval":0.327128,"zval":0.698530}}]}

console.log('Loading event');
var AWS = require('aws-sdk');
var dynamodb = new AWS.DynamoDB();

exports.handler = function(event, context) {

    console.log("Request received:\n", JSON.stringify(event));
    console.log("Context received:\n", JSON.stringify(context));

    var tableName = "[TABLE_NAME]";
    var lambdaFunctionArn = "arn:aws:lambda:us-west-2:555818481905:function:ProcessDeviceData-connected-maraca";
    var lambdaRegion = "us-west-2";
    var datetime = new Date().getTime().toString();

    event.Records.forEach(function(record) {
        asciidata = new Buffer(record.kinesis.data, 'base64').toString('ascii');
        payload = JSON.parse(asciidata);
        console.log("Decoded Data:", payload);

        dynamodb.putItem({
            "TableName": tableName,
            "Item": {
                "device_id": {
                    "S": payload.device_id
                },
                "time": {
                    "N": JSON.stringify(payload.time)
                },
                "device": {
                    "S": payload.device
                },
                "sensors": {
                    "S": JSON.stringify(payload.sensors[0].telemetryData)
                },
                "source": {
                    "S": "from lambda"
                }
            }
        }, function(err, data) {
            if (err) {
                context.fail('ERROR: Putting item into dynamodb failed: ' + err);
            } else {
                console.log('Great Success! JSON: ' + JSON.stringify(data, null, '  '));
            }
        });
    });

    var params = {
        FunctionName: lambdaFunctionArn,
        InvocationType: 'RequestResponse',
        LogType: 'None',
        Payload: JSON.stringify(payload)
    };

    var lambda = new AWS.Lambda({
        region: lambdaRegion
    });
    lambda.invoke(params, function(err, data) {
        if (err) {
            context.fail('ERROR:Invoking Lambda function: ' + err)
        } else {
            console.log('Great success! JSON: ' + JSON.stringify(data, null, ' '));
            context.succeed('SUCCESS');
        }
    });
};
