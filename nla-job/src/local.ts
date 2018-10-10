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
    handler(event).
      on('success', function(response) {
        console.log("Success!");
      }).
      on('error', function(response) {
        console.log("Error!");
      }).
      on('complete', function(response) {
        console.log("Always!");
      });
}

run({
    Records: []
});