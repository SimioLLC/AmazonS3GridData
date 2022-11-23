using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace AmazonS3CSVGridData
{
    static class Utils
    {
        private static IAmazonS3 s3Client;

        public static void UploadFile(string regionEndpointName, string accessKeyID, string secretAccessKey, string bucketName, string keyName, Stream stream)
        {
            s3Client = new AmazonS3Client(accessKeyID, secretAccessKey, GetRegionEndpoint(regionEndpointName));
            var fileTransferUtility = new TransferUtility(s3Client);
            fileTransferUtility.Upload(stream, bucketName, keyName);
        }

        public static void DownloadFileToFile(string regionEndpointName, string accessKeyID, string secretAccessKey, string bucketName, string keyName, string fileName)
        {
            s3Client = new AmazonS3Client(accessKeyID, secretAccessKey, GetRegionEndpoint(regionEndpointName));
            var fileTransferUtility = new TransferUtility(s3Client);
            fileTransferUtility.Download(fileName, bucketName, keyName);
        }

        internal static StreamReader DownloadFile(string regionEndpointName, string accessKeyID, string secretAccessKey, string bucketName, string keyName)
        {
            try
            {
                s3Client = new AmazonS3Client(accessKeyID, secretAccessKey, GetRegionEndpoint(regionEndpointName));

                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                var ms = new MemoryStream();
                using (var res = s3Client.GetObject(request))
                using (var responseStream = res.ResponseStream)
                {                
                    responseStream.CopyTo(ms);
                }
                return new StreamReader(ms);
            }
            catch (AmazonS3Exception e)
            {                
                // If bucket or object does not exist
                string exceptionMessage = String.Format("Error encountered ***. Message:'{0}' when reading object", e.Message);
                throw new Exception(exceptionMessage);
            }
            catch (Exception e)
            {
                string exceptionMessage = String.Format("Unknown encountered on server. Message:'{0}' when reading object", e.Message);
                throw new Exception(exceptionMessage);
            }
        }

        internal static RegionEndpoint GetRegionEndpoint(string regionEndpointName)
        {
            // Display each row and column value.
            foreach (RegionEndpoint regEndpoint in RegionEndpoint.EnumerableAllRegions)
            {                
                if (regionEndpointName == regEndpoint.SystemName)
                {
                    return regEndpoint;
                }
            }

            return null;
        }

        internal static string[] GetListOfRegionEndpoints()
        {
            // Display each row and column value.
            List<String> regionEnpointNames = new List<String>();
            foreach (RegionEndpoint regEndpoint in RegionEndpoint.EnumerableAllRegions)
            {
                regionEnpointNames.Add(regEndpoint.SystemName);
            }

            return regionEnpointNames.ToArray();
        }
    }
}
