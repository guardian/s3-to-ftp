import { handler } from './lambda';
let AWS = require('aws-sdk');

/**
 * For testing locally:
 * `yarn run local <source bucket> <object key> <year> <month> <day>`
 */

AWS.config = new AWS.Config({
    region: 'eu-west-1',
    credentialProvider: new AWS.CredentialProviderChain(new AWS.SharedIniFileCredentials({ profile: 'ophan' }))
});

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
    ],
    When: `${process.argv[4]}-${process.argv[5]}-${process.argv[6]}`
});
