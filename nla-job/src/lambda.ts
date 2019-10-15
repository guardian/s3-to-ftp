import {Config} from './config';

let AWS = require('aws-sdk');
const sts = new AWS.STS({ apiVersion: '2011-06-15' });

const config = new Config();

export function handler() {
  const when = new Date();
  when.setDate(when.getDate() - 1);

  return new Promise((resolve, reject) => {
    console.log(`Assuming role ${config.AthenaRole}...`)
    sts.assumeRole({
      RoleArn: config.AthenaRole,
      RoleSessionName: 'ophan'
    }, (err, data) => {
        if (err) {
            console.error(`Can't assume role ${config.AthenaRole}`, err)
            reject(err);
        } else {
            resolve(new AWS.Athena({
              region: 'eu-west-1',
              accessKeyId: data.Credentials.AccessKeyId,
              secretAccessKey: data.Credentials.SecretAccessKey,
              sessionToken: data.Credentials.SessionToken
            }));
        }
    });
  })
    .then(athena => runQuery(athena, when))
    .catch(err => {
      console.log(`Something went wrong: ${err}`)
    });
}

export function runQuery(athena: any, when: Date) {
  console.log(`Getting data for ${when}...`)

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

