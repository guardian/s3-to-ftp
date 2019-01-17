import { handler } from './lambda';
let AWS = require('aws-sdk');

const args = process.argv.slice(2);
if (args[0] && args[1] && args[2]) {
  const when = new Date(`${args[0]}-${args[1]}-${args[2]}`);
  handler(when);
} else {
  handler(null);
}

