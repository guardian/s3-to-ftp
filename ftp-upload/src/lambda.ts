import {Config} from './config';
import {Readable} from "stream";

const AWS = require('aws-sdk');
const ftp = require('ftp');
const fs = require('fs');
const archiver = require('archiver');

const config = new Config();
const sts = new AWS.STS({ apiVersion: '2011-06-15' });

export async function handler(event) {
    return new Promise((resolve, reject) => {
        console.log(`Assuming role ${config.AthenaRole}`)
        sts.assumeRole({
            RoleArn: config.AthenaRole,
            RoleSessionName: 'ophan'
        }, (err, data) => {
            if (err) 
                reject(err);
            else {
                AWS.config.update({
                    accessKeyId: data.Credentials.AccessKeyId,
                    secretAccessKey: data.Credentials.SecretAccessKey,
                    sessionToken: data.Credentials.SessionToken
                });
                resolve(event);
            }
        });
    }).then(run);
}

async function run(event) {
    const s3 = new AWS.S3();

    return Promise.all(event.Records
        .filter(record => record.s3.object.key.endsWith('csv'))
        .slice(0, 1)
        .map(record => {
            const bucket = record.s3.bucket.name;
            const key = record.s3.object.key;
            let when;
            if (event.When && /\d{4}-\d{2}-\d{2}/.test(event.When)) {
                when = new Date(event.When)
            } else {
                when = new Date();
                // this is how you do date math in js: just add or substract whichever field is necessary
                when.setDate(when.getDate() - 1);
            }
            // produce theguardian_YYYYMMDD (months are zero-indexed in js)
            const destinationPath = `theguardian_${when.getFullYear()}${pad(when.getMonth() + 1)}${pad(when.getDate())}`;
            console.log(`Streaming ${bucket}/${key} to ${destinationPath}.zip`);

            return streamS3ToLocalZip(bucket, key, `${destinationPath}.csv`)
                .then(fileName => 
                    ftpConnect(config.FtpHost, config.FtpUser, config.FtpPassword)
                        .then(ftpClient => streamLocalToFtp(fileName, `${destinationPath}.zip`, ftpClient))
                );
        })
    )

    /**
     * Creates a new ftp connection and returns the ftp client.
     */
    function ftpConnect(host: string, user: string, password: string): Promise<any> {
        return new Promise((resolve, reject) => {
            const ftpClient = new ftp();

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
                    } else {
                        ftpClient.end();
                        console.log(`Successfully uploaded ${path} to NLA`)
                        resolve(path);
                    }
                });
            });

            stream.on('error', (err: Error) => {
                console.log(`Error streaming ${path} to ftp`, err);
                reject(err)
            });
        })
    }

    /**
     * Streams the given s3 object to a local zip archive.
     */
    function streamS3ToLocalZip(bucket: string, key: string, dst: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const stream: Readable = s3.getObject({
                Bucket: bucket,
                Key: key
            }).createReadStream();

            const outputFile = `/tmp/${key}.zip`;

            const output = fs.createWriteStream(outputFile);
            const archive = archiver('zip');

            archive.pipe(output);

            archive.append(stream, { name: dst });
            archive.finalize();

            stream.on('end', () => {
                resolve(outputFile);
            });

            stream.on('error', (err: Error) => {
                console.log(`Error streaming ${key} to archive`, err);
                reject(err);
            });

            archive.on('error', (err) => {
                console.log(`Error archiving ${key}`, err);
                reject(err);
            });
        })
    }

    /**
     * Streams the given local file to the given ftp session.
     */
    function streamLocalToFtp(path: string, dst: string, ftpClient): Promise<string> {
        const stream = fs.createReadStream(path);

        return streamToFtp(stream, dst, ftpClient)
    }
}

function pad(n: number): string {
    return n.toString(10).padStart(2, '0');
}
