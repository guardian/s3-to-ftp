import { runQuery } from './lambda';
let AWS = require('aws-sdk');

AWS.config = new AWS.Config();
AWS.config.accessKeyId = process.env.AWS_ACCESS_KEY_ID;
AWS.config.secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
AWS.config.sessionToken = process.env.AWS_SESSION_TOKEN;
AWS.config.region = "eu-west-1";

const args = process.argv.slice(2);

const when = new Date(`${args[0]}-${args[1]}-${args[2]}`);
runQuery(new AWS.Athena({ region: 'eu-west-1' }), when);
