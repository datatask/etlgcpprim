from google.cloud import storage, logging
from google.cloud.exceptions import NotFound
import os
import time


class Etlstorage(storage.Client):
    """ Class to manage storage etl primitive transformations
    """

    def __init__(self, project_id, logger, labels={}):
        super().__init__(project=project_id)
        self.logger = logger
        self.labels = labels

    @staticmethod
    def extractBucketFile(gsPath):
        """
        Function to separate the bucket and the file name from the full path file in GCS
        Input :
            gsPath : full path file
        Output :
            bucket : bucket name
            filePath : filename, with folders if presents
        """
        bucket = gsPath.replace("gs://", "").split("/")[0]
        filePath = gsPath[len(bucket) + 6 :]
        return bucket, filePath

    def file_exist_gs(self, gsPath):
        """
        Function to know if a file is contained in a GCS bucket
        Input :
            gsPath : full path file
        Output :
            stat : True if present, False otherwise
        """
        try:
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            stat = storage.Blob(bucket=bucket, name=fileName).exists(self)
            self.logger.log_text(
                text=f"File {fileName} available in bucket {bucketName} : {stat}",
                severity="INFO",
                labels=self.labels,
            )
            return stat
        except Exception as e:
            self.logger.log_text(
                text=f"Error in checking file {fileName} in GS : {e}",
                severity="ERROR",
                labels=self.labels,
            )
            return False

    def move_file_gs(self, gsPathIn, gsPathOut, delete=True, retry=0):
        """
        Function to move a file from a bucket into another bucket/folder
        Input :
            gsPathIn : full input path file
            gsPathOut : full output path file
            delete : True if input file is deleted False otherwise
        Output :
            True if move has been done, False otherwise
        """
        try:
            bucketIn, fileNameIn = self.extractBucketFile(gsPathIn)
            bucketOut, fileNameOut = self.extractBucketFile(gsPathOut)
            inputBucket = self.get_bucket(bucketIn)
            inputBlob = inputBucket.blob(fileNameIn)
            outputBucket = self.get_bucket(bucketOut)
            inputBucket.copy_blob(inputBlob, outputBucket, new_name=fileNameOut)
            if delete:
                inputBlob.delete()
                self.logger.log_text(
                    text=f"Input file {gsPathIn} deleted",
                    severity="INFO",
                    labels=self.labels,
                )
            self.logger.log_text(
                text=f"File move GCS input {gsPathIn} :: output {gsPathOut}",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except NotFound:
            self.logger.log_text(
                text=f"File not found when moving file in GS :: gsPath {gsPathIn}",
                severity="WARNING",
                labels=self.labels,
            )
            return False
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in file move GCS :: input {gsPathIn} :: retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(3)
                return self.move_file_gs(gsPathIn, gsPathOut, delete, retry=retry + 1)
            else:
                self.logger.log_text(
                    text=f"Error in file move GCS {e} :: input {gsPathIn} :: Max number of attemps exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return False

    def copy_file_gs(self, gsPathIn, gsPathOut):
        """
        Function to copy a file from a bucket into another bucket/folder
        Input :
            gsPathIn : full input path file
            gsPathOut : full output path file
        Output :
            True if copy has been done, False otherwise
        """
        return self.move_file_gs(gsPathIn, gsPathOut, delete=False)

    def rename_gs(self, gsPath, newName):
        """
        Function to rename a file in GCS
        Input :
            gsPath: full input path file
            newName : new path file name
        Output :
            outputGsPath : output full GCS path
        """
        try:
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            inputBlob = bucket.blob(fileName)
            bucket.rename_blob(inputBlob, newName)
            self.logger.log_text(
                text=f"File {gsPath} renamed to {newName}",
                severity="INFO",
                labels=self.labels,
            )
            outputGsPath = "gs://" + bucketName + "/" + newName
            return outputGsPath
        except Exception as e:
            self.logger.log_text(
                text=f"Error in renaming file {gsPath} : {e}",
                severity="ERROR",
                labels=self.labels,
            )
            return False

    def download_file_from_gs(self, gsPath, localPath, retry=0):
        """
        Function to download locally a file from GCS
        Input :
            gsPath: full gs input path file
            localPath : desired local path of the file
        Output :
            True if the file has been downloaded, False otherwise
        """
        try:
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            blob = bucket.get_blob(fileName)
            blob.download_to_filename(localPath)
            self.logger.log_text(
                text=f"File saved locally from GS :: gsPath {localPath}",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except NotFound:
            self.logger.log_text(
                text=f"File not found when downloading file from GS :: gsPath {gsPath}",
                severity="WARNING",
                labels=self.labels,
            )
            return False
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in downloading file from GS :: localPath {localPath} :: gsPath {gsPath} :: retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(3)
                return self.download_file_from_gs(gsPath, localPath, retry=retry + 1)
            else:
                self.logger.log_text(
                    text=f"Error in downloading file from GS :: gsPath {gsPath} :: {e}. Max number of retry exceeded",
                    severity="ERROR",
                    labels=self.labels,
                )
                return False

    def upload_file_to_gs(self, localPath, gsPath, delete=False, retry=0):
        """
        Function to upload a file, locally stored, on GCS
        Input :
            localPath : local path of the file
            gsPath: full gs output path file
            delete : True if the file will be deleted locally after being uploaded, False otherwise
        Output :
            True if the file has been uploaded, False otherwise
        """
        try:
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            blob = bucket.blob(fileName)
            blob.upload_from_filename(localPath)
            if delete:
                os.remove(localPath)
                self.logger.log_text(
                    text=f"Local file deleted : {localPath}",
                    severity="INFO",
                    labels=self.labels,
                )
            self.logger.log_text(
                text=f"File upload to GS :: gsPath {gsPath}",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except Exception as e:
            if retry < 3:
                self.logger.log_text(
                    text=f"Error in uploading file to GS :: local_path {localPath} :: gsPath {gsPath} :: retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(3)
                return self.upload_file_to_gs(
                    localPath, gsPath, delete, retry=retry + 1
                )
            else:
                self.logger.log_text(
                    text=f"Error in uploading file to GS :: local_path {localPath} :: {e}",
                    severity="ERROR",
                    labels=self.labels,
                )
                return False

    def list_files_from_gs(self, gsPath, recurse=False):
        """
        Function to list the files in a GCS folder
        Input :
            gsPath: full gs path to scan
            recurse: recurse list of files if True
        Output :
            list of the files contained in the path (and child folders if recurse=True)
        """
        try:
            if gsPath[-1] != "/":
                gsPath = gsPath + "/"
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            if recurse:
                allfiles = [
                    "gs://" + bucketName + "/" + f.name
                    for f in bucket.list_blobs(prefix=fileName, fields="items/name")
                    if f.name[-1] != "/"
                ]
            else:
                allfiles = [
                    "gs://" + bucketName + "/" + f.name
                    for f in bucket.list_blobs(
                        prefix=fileName, fields="items/name", delimiter="/"
                    )
                    if f.name[-1] != "/"
                ]
            self.logger.log_text(
                text=f"Listing {len(allfiles)} files from gsPath {gsPath} in recurse = {recurse} mode.",
                severity="INFO",
                labels=self.labels,
            )
            return allfiles
        except Exception as e:
            self.logger.log_text(
                text=f"Error in list files from GS :: gsPath {gsPath} :: {e}",
                severity="ERROR",
                labels=self.labels,
            )
            return None

    def delete_file_from_gs(self, gsPath, retry=0):
        """
        Function to delete a file from GCS
        Input :
            gsPath: full gs path from file to delete
        Output :
            True if the file has been deleted, False otherwise
        """
        try:
            bucketName, fileName = self.extractBucketFile(gsPath)
            bucket = self.get_bucket(bucketName)
            blob = bucket.blob(fileName)
            blob.delete()
            self.logger.log_text(
                text=f"File deleted from GS :: gsPath {fileName}",
                severity="INFO",
                labels=self.labels,
            )
            return True
        except NotFound:
            self.logger.log_text(
                text=f"File not found when deleting file from GS :: gsPath {gsPath}",
                severity="WARNING",
                labels=self.labels,
            )
            return False
        except Exception as e:
            if retry < 2:
                self.logger.log_text(
                    text=f"Error in deleting file from GS :: gsPath {gsPath} :: retry {retry + 1}",
                    severity="WARNING",
                    labels=self.labels,
                )
                time.sleep(3)
                return self.delete_file_from_gs(gsPath, retry=retry + 1)
            else:
                self.logger.log_text(
                    text=f"Error in deleting file from GS :: gsPath {gsPath} :: {e}",
                    severity="ERROR",
                    labels=self.labels,
                )
                return False


if __name__ == "__main__":

    ### vars to be define by reading a parameter file or env vars ...
    project_id = os.environ.get("PROJECT_ID", "")
    logger_name = os.environ.get("LOGGERNAME", "etlgcp")

    ### set the GOOGLE_APPLICATION_CREDENTIALS env var for service account authentication
    logging_client = logging.Client(project=project_id)
    logger = logging_client.logger(logger_name)
    etlst = Etlstorage(project_id, logger, {"test": "myvalue"})

    ### example
    # etlst.labels={"test2":"setlabel"}
    # etlst.file_exist_gs("gs://allconfigfiles/fournisseursv2.yaml")
    # print(etlst.list_files_from_gs("gs://allconfigfiles"))

