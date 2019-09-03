import {Config} from './config';
import {Readable} from "stream";

const AWS = require('aws-sdk');
const FtpClient = require('ftp');
const fs = require('fs');
const archiver = require('archiver');

const config = new Config();
const cloudwatch = new AWS.CloudWatch({ apiVersion: '2010-08-01' });
const sts = new AWS.STS({ apiVersion: '2011-06-15' });

export async function handler(event) {
    return new Promise((resolve, reject) => {
        console.log(`Assuming role ${config.AthenaRole}`)
        sts.assumeRole({
            RoleArn: config.AthenaRole,
            RoleSessionName: 'ophan'
        }, (err, data) => {
            if (err) {
                console.error(`Can't assume role ${config.AthenaRole}`, err)
                reject(err);
            } else {
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
                .then(fileName => {
                    const destination: string = destinationPath + '.zip';
                    return Promise.all([
                        uploadToNLA(fileName, destination),
                        uploadToS3(config.DestinationBucket, fileName, destination)
                    ]);
                });
        })
    )

    async function uploadToNLA(fileName: string, destination: string): Promise<string> {
        return ftpConnect(config.FtpHost, config.FtpUser, config.FtpPassword)
            .then(ftpClient => streamLocalToFtp(fileName, destination, ftpClient));
    }

    async function uploadToS3(bucket: String, fileName: string, destination: string): Promise<void> {
        return new Promise((resolve, reject) => {
            s3.putObject({
                Body: fs.createReadStream(fileName),
                Bucket: bucket,
                Key: destination,
                ContentEncoding: 'deflate',
                ContentType: 'text/csv'
            }, (err) => {
                if (err) {
                    console.error(`Could not upload ${fileName} to S3: ${err}`);
                    reject(err);
                } else {
                    console.log(`Successfully uploaded ${fileName} to S3 as ${bucket}/${destination}`);
                    resolve();
                }
            });
        });
    }


    /**
     * Creates a new ftp connection and returns the ftp client.
     */
    function ftpConnect(host: string, user: string, password: string): Promise<object> {
        return new Promise((resolve, reject) => {
            const ftpClient = new FtpClient();

            ftpClient.on('ready', () => {
                console.log("And we're in!");
                resolve(ftpClient);
            });

            ftpClient.on('error', (err) => {
                console.log(`"FTP error: ${err}`);
                reject(err);
            });

            console.log(`Connecting to ${host}...`);

            ftpClient.connect({
                host,
                user,
                password
            });
        })
    }

    /**
     * Streams the given s3 object to a local zip archive.
     */
    function streamS3ToLocalZip(bucket: string, key: string, dst: string): Promise<string> {
        return new Promise((resolve, reject) => {
            const request = s3.getObject({
                Bucket: bucket,
                Key: key
            }, (error, data) => {
                if (error) {
                    console.error(`Failed to fetch ${bucket}/${key}: ${error}`);
                    reject(error);
                } else {
                    const fileSizeInMB = data.ContentLength/1024/1024;
                    console.log(`Received ${bucket}/${key} (${fileSizeInMB.toFixed(2)}MB, ${data.ContentType})`);
                    sendToCloudwatch('SizeOfCSV', fileSizeInMB, 'Megabytes').then(() => resolve(data.Body));
                }
            });
            
            request.send();
        }).then((stream: ReadableStream) => new Promise((resolve, reject) => {
            const outputFile = `/tmp/${key}.zip`;
            const output = fs.createWriteStream(outputFile);
            const archive = archiver('zip');

            output.on('close', () => {
                console.log(`Finished zipping CSV file to ${outputFile} (${(archive.pointer()/1024/1024).toFixed(2)}MB)`);
                resolve(outputFile);
            });

            // good practice to catch warnings (ie stat failures and other non-blocking errors)
            archive.on('warning', (err) => {
                if (err.code === 'ENOENT') {
                    console.warn("Woopsie, something weird happened", err);
                } else {
                    console.error(`Error archiving ${key} to ${outputFile}`, err);
                    reject(err);
                }
            });

            archive.on('error', (err) => {
                console.log(`Error archiving ${key}`, err);
                reject(err);
            });

            console.log(`Zipping ${bucket}/${key} into ${outputFile}`);

            archive.pipe(output);
            archive.append(stream, { name: dst });
            archive.finalize();
        }));
    }

    /**
     * Streams the given local file to the given ftp session.
     */
    function streamLocalToFtp(path: string, dst: string, ftpClient): Promise<string> {
        return new Promise((resolve, reject) => {
            console.log(`Now uploading ${path} to ${dst}`);

            ftpClient.put(path, dst, (err) => {
                if (err) {
                    console.log(`Error writing ${dst} to ftp`, err);
                    reject(err);
                } else {
                    ftpClient.end();
                    console.log(`Successfully uploaded ${dst} to NLA`);
                    resolve(dst);
                }
            });
        })
    }

    function sendToCloudwatch(metricName: String, value: number, unit: string): Promise<void> {
        return new Promise(resolve => {
            cloudwatch.putMetricData({
                MetricData: [{
                    MetricName: metricName,
                    Value: value,
                    Unit: unit
                }]
            }, function() {
                resolve();
            })
        });
    }
}

function pad(n: number): string {
    return n.toString(10).padStart(2, '0');
}
