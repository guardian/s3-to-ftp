import {Config} from './config';

let AWS = require('aws-sdk');

const config = new Config();

export function handler(when: any) {
  if (!(when instanceof Date)) {
    when = new Date();
    when.setDate(when.getDate() - 1);
  }
  console.log(`Assuming role ${config.AthenaRole}...`)
  const chain = new AWS.CredentialProviderChain();
  chain.providers.unshift(() => new AWS.TemporaryCredentials({
    RoleArn: config.AthenaRole,
    RoleSessionName: 'ophan'
  }));
  chain.providers.unshift(() => new AWS.SharedIniFileCredentials({ profile: 'ophan' }))
  chain.resolve(runQuery.bind(null, when));
}

function runQuery(when: Date, err, credentials) {
  if (err) console.log(err, err.stack);
  else {
    console.log(`Getting data for ${when}...`)
    let athena = new AWS.Athena({ 
      region: 'eu-west-1', 
      accessKeyId: credentials.accessKeyId,
      secretAccessKey: credentials.secretAccessKey,
      sessionToken: credentials.sessionToken
    });
    
    const query = `
      SELECT web_title, url_raw, pv.path AS path, count(1) AS pageviews
      FROM   clean.pageview pv
            INNER JOIN clean.content_new c ON pv.path = c.path
            CROSS JOIN UNNEST (content_type_tag) AS A (a_type_tag)
            CROSS JOIN UNNEST (tone_tag) AS A (a_tone_tag)
      WHERE  received_date = date'${when.toISOString().slice(0, 10)}'
      AND    platform = 'NEXT_GEN'
      AND    (a_type_tag = 'type/article' OR a_tone_tag = 'tone/minutebyminute')
      GROUP BY 1,2,3
      ORDER BY 4 DESC
    `;

    const params = {
      QueryString: query,
      ResultConfiguration: {
        OutputLocation: config.Destination,
      },
      ClientRequestToken: `request-${when.toString()}`,
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

