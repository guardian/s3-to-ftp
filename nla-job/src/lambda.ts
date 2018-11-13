import {Config} from './config';

let AWS = require('aws-sdk');

const config = new Config();

export function handler() {
  const chain = new AWS.CredentialProviderChain();
  chain.providers.unshift(() => new AWS.TemporaryCredentials({
    RoleArn: config.AthenaRole,
    RoleSessionName: 'ophan'
  }));
  chain.providers.unshift(() => new AWS.SharedIniFileCredentials({ profile: 'ophan' }))
  chain.resolve(runQuery);
}

export function runQuery(err, credentials) {
  if (err) console.log(err, err.stack);
  else {
    let athena = new AWS.Athena({ 
      region: 'eu-west-1', 
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
      sessionToken: credentials.sessionToken
    });
    
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const query = `
      SELECT web_title, url_raw, pv.path AS url_path, count(1) AS pageviews, received_date
      FROM   clean.pageview pv
            INNER JOIN clean.content_new c ON pv.path = c.path
            CROSS JOIN UNNEST (content_type_tag) AS A (a_type_tag)
            CROSS JOIN UNNEST (tone_tag) AS A (a_tone_tag)
      WHERE  received_date = date'${yesterday.toISOString().slice(0, 10)}'
      AND    platform = 'NEXT_GEN'
      AND    (a_type_tag = 'type/article' OR a_tone_tag = 'tone/minutebyminute')
      GROUP BY 1,2,3,5
      ORDER BY 4 DESC
    `;

    const params = {
      QueryString: query,
      ResultConfiguration: {
        OutputLocation: config.Destination,
      },
      ClientRequestToken: yesterday.toString(),
      QueryExecutionContext: {
        Database: config.Schema
      }
    };

    return athena.startQueryExecution(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(data);           // successful response
    });
  }
}

