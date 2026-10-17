using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

namespace DataIngestor.Storage
{
    // any S3-compatible server (MinIO today); keys come from user-secrets or env vars, never appsettings.json
    public class S3FlightStore : IFlightStore, IDisposable
    {
        private const string SERVICE_URL_FIELD = "Storage:ServiceUrl";
        private const string BUCKET_FIELD = "Storage:Bucket";
        private const string ACCESS_KEY_FIELD = "Storage:AccessKey";
        private const string SECRET_KEY_FIELD = "Storage:SecretKey";

        private readonly AmazonS3Client _client;
        private readonly string _bucket;

        public S3FlightStore(IConfiguration configuration)
        {
            string serviceUrl = configuration[SERVICE_URL_FIELD] ?? throw new InvalidOperationException($"{SERVICE_URL_FIELD} is not configured");
            _bucket = configuration[BUCKET_FIELD] ?? throw new InvalidOperationException($"{BUCKET_FIELD} is not configured");
            string accessKey = configuration[ACCESS_KEY_FIELD] ?? throw new InvalidOperationException($"{ACCESS_KEY_FIELD} is not configured");
            string secretKey = configuration[SECRET_KEY_FIELD] ?? throw new InvalidOperationException($"{SECRET_KEY_FIELD} is not configured");

            // MinIO serves buckets as a path (host/bucket/key), not as a subdomain the way AWS does
            AmazonS3Config config = new() { ServiceURL = serviceUrl, ForcePathStyle = true };
            _client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), config);
        }

        public async Task PutAsync(string key, Stream content, string contentType, CancellationToken cancellationToken)
        {
            PutObjectRequest request = new()
            {
                BucketName = _bucket,
                Key = key,
                InputStream = content,
                ContentType = contentType,
                AutoCloseStream = false
            };

            await _client.PutObjectAsync(request, cancellationToken);
        }

        public async Task DeletePrefixAsync(string prefix, CancellationToken cancellationToken)
        {
            ListObjectsV2Request listRequest = new() { BucketName = _bucket, Prefix = prefix };
            ListObjectsV2Response listResponse;
            do
            {
                // a listing page holds at most 1000 keys, which is also the most one delete call accepts
                listResponse = await _client.ListObjectsV2Async(listRequest, cancellationToken);
                if (listResponse.S3Objects is { Count: > 0 } objects)
                {
                    DeleteObjectsRequest deleteRequest = new()
                    {
                        BucketName = _bucket,
                        Objects = objects.Select(o => new KeyVersion { Key = o.Key }).ToList()
                    };
                    await _client.DeleteObjectsAsync(deleteRequest, cancellationToken);
                }

                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated == true);
        }

        public void Dispose() => _client.Dispose();
    }
}
