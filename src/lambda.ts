import {Config} from './config';
import {Readable} from "stream";

let AWS = require('aws-sdk');
let ftp = require('ftp');

let s3 = new AWS.S3();
let config = new Config();

export async function handler(event) {
    return Promise.all(
        event.Records.map(record => {
            let bucket = record.s3.bucket.name;
            let key = record.s3.object.key;
            console.log(`Streaming ${bucket}/${key}`);

            return ftpConnect(config.FtpHost, config.FtpUser, config.FtpPassword)
                .then((ftpClient) => streamFile(bucket, key, ftpClient))
        })
    )
}

/**
 * Creates a new ftp connection and returns the ftp client.
 */
function ftpConnect(host: string, user: string, password: string): Promise<any> {
    return new Promise((resolve, reject) => {
        let ftpClient = new ftp();

        ftpClient.connect({
            host,
            user,
            password
        });

        ftpClient.on('ready', () => {
            resolve(ftpClient)
        });
        ftpClient.on('error', (err) => {
            console.log("FTP error: ", err);
            reject(err)
        });
    })
}

/**
 * Streams the given s3 object to the given ftp session.
 */
function streamFile(bucket: string, key: string, ftpClient): Promise<string> {
    return new Promise((resolve, reject) => {
        let stream: Readable = s3.getObject({
            Bucket: bucket,
            Key: key
        }).createReadStream();

        stream.on('readable', () => {
            ftpClient.put(stream, key, (err) => {
                if (err) {
                    console.log(`Error writing ${key} to ftp`, err);
                    reject(err)
                }
            })
        });

        stream.on('end', () => {
            resolve(key)
        });

        stream.on('error', (err: Error) => {
            console.log(`Error streaming ${key}`, err);
            reject(err)
        });
    })
}
