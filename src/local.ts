import { handler } from './lambda';
let AWS = require('aws-sdk');

/**
 * For testing locally:
 * `yarn run local <source bucket> <object key>`
 */

AWS.config = new AWS.Config();
AWS.config.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
AWS.config.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
AWS.config.sessionToken = process.env.AWS_SESSION_TOKEN;
AWS.config.region = "eu-west-1";

async function run(event) {
    await handler(event)
        .then(result => console.log(`Result: ${result}`))
        .catch(err => console.log(`Error: ${err}`))
}

run({
    Records: [
        {
            s3: {
                bucket: {
                    name: process.argv[2]
                },
                object: {
                    key: process.argv[3]
                }
            }
        }
    ]
});
