import {Config} from './config';

let AWS = require('aws-sdk');
let config = new Config();

export function handler() {
  let sts = new AWS.STS();
  sts.assumeRole({
    RoleArn: config.AthenaRole,
    RoleSessionName: 'capi-nla-athena-to-s3'
  }, (error, data) => {
    if (error) console.log(error, error.stack);
    else runQuery(data.Credentials);
  });
}

export function runQuery(creds) {
  let athena = new AWS.Athena({ ... creds });
  
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

