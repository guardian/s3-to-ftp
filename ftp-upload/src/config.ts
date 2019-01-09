export class Config {
    FtpHost: string = process.env.FtpHost;
    FtpUser: string = process.env.FtpUser;
    FtpPassword: string = process.env.FtpPassword;
    ZipFile: boolean = process.env.ZipFile.toLowerCase() === "true";
    AthenaRole: string = process.env.AthenaRole;
}
