import {Config} from './config';
import {Readable} from "stream";

let AWS = require('aws-sdk');
let ftp = require('ftp');
let fs = require('fs');
let archiver = require('archiver');
const sts = new AWS.STS();

let config = new Config();

export async function handler(event) {
    console.log(`Assuming role...`);
    sts.assumeRole({
        RoleArn: config.AthenaRole
    }, getCredentials(event))    
}    

const getCredentials = event => (err, data) => {
    if (err) {
        console.error(err, err.stack);
    } else {
        processRecords(event, data.Credentials);
    }
}

const processRecords = (event, credentials) => {
    const s3 = new AWS.S3({
        apiVersion: 'latest',
        region: 'eu-west-1', 
        credentials
    });

    return Promise.all(
        event.Records.map(record => {
            let bucket = record.s3.bucket.name;
            let key = record.s3.object.key;
            console.log(`Streaming ${bucket}/${key}`);

            if (config.ZipFile) {
                return streamS3ToLocalZip(bucket, key)
                    .then(() => ftpConnect(config.FtpHost, config.FtpUser, config.FtpPassword))
                    .then(ftpClient => streamLocalToFtp(`${key}.zip`, ftpClient))
            } else {
                return ftpConnect(config.FtpHost, config.FtpUser, config.FtpPassword)
                    .then(ftpClient => streamS3FileToFtp(bucket, key, ftpClient))
            }
        })
    );

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
     * Pipes the stream to the ftp session under the given path.
     */
    function streamToFtp(stream: Readable, path: string, ftpClient): Promise<string> {
        return new Promise((resolve, reject) => {
            stream.on('readable', () => {
                ftpClient.put(stream, path, (err) => {
                    if (err) {
                        console.log(`Error writing ${path} to ftp`, err);
                        reject(err)
                    }
                })
            });

            stream.on('end', () => {
                resolve(path)
            });

            stream.on('error', (err: Error) => {
                console.log(`Error streaming ${path} to ftp`, err);
                reject(err)
            });
        })
    }

    /**
     * Streams the given s3 object to the given ftp session.
     */
    function streamS3FileToFtp(bucket: string, key: string, ftpClient): Promise<string> {
        let stream: Readable = s3.getObject({
            Bucket: bucket,
            Key: key
        }).createReadStream();

        return streamToFtp(stream, key, ftpClient)
    }

    /**
     * Streams the given s3 object to a local zip archive.
     */
    function streamS3ToLocalZip(bucket: string, key: string): Promise<string> {
        return new Promise((resolve, reject) => {
            let stream: Readable = s3.getObject({
                Bucket: bucket,
                Key: key
            }).createReadStream();

            let output = fs.createWriteStream(`${key}.zip`);
            let archive = archiver('zip');

            archive.pipe(output);

            archive.append(stream, { name: key });
            archive.finalize();

            stream.on('end', () => {
                resolve(key)
            });

            stream.on('error', (err: Error) => {
                console.log(`Error streaming ${key} to archive`, err);
                reject(err)
            });

            archive.on('error', (err) => {
                console.log(`Error archiving ${key}`, err);
                reject(err)
            });
        })
    }

    /**
     * Streams the given local file to the given ftp session.
     */
    function streamLocalToFtp(path: string, ftpClient): Promise<string> {
        let stream = fs.createReadStream(path);

        return streamToFtp(stream, path, ftpClient)
    }
}