using System.Text;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using DataIngestor.Storage;
using Microsoft.Extensions.Configuration;

// talks to the real MinIO; skipped unless the Storage keys are set (user-secrets or Storage__AccessKey / Storage__SecretKey)
public class S3FlightStoreLiveTests
{
    private static IConfiguration LoadConfiguration() => new ConfigurationBuilder()
        .AddJsonFile("appsettings.json", optional: true)
        .AddUserSecrets(typeof(S3FlightStore).Assembly, optional: true)
        .AddEnvironmentVariables()
        .Build();

    [SkippableFact]
    public async Task PutThenDeletePrefix_RoundTripsAgainstTheRealServer()
    {
        IConfiguration configuration = LoadConfiguration();
        Skip.If(string.IsNullOrEmpty(configuration["Storage:AccessKey"]) || string.IsNullOrEmpty(configuration["Storage:ServiceUrl"]),
            "Storage keys not configured; set them to run against MinIO.");

        string prefix = $"_livetest/{Guid.NewGuid():N}/";
        using S3FlightStore store = new(configuration);
        using AmazonS3Client client = new(
            new BasicAWSCredentials(configuration["Storage:AccessKey"], configuration["Storage:SecretKey"]),
            new AmazonS3Config { ServiceURL = configuration["Storage:ServiceUrl"], ForcePathStyle = true });
        string bucket = configuration["Storage:Bucket"]!;

        await store.PutAsync(prefix + "a.json", new MemoryStream(Encoding.UTF8.GetBytes("{\"hello\":\"minio\"}")), "application/json", CancellationToken.None);
        await store.PutAsync(prefix + "nested/b.json", new MemoryStream(Encoding.UTF8.GetBytes("{}")), "application/json", CancellationToken.None);

        using (GetObjectResponse got = await client.GetObjectAsync(bucket, prefix + "a.json"))
        using (StreamReader reader = new(got.ResponseStream))
            Assert.Equal("{\"hello\":\"minio\"}", await reader.ReadToEndAsync());

        await store.DeletePrefixAsync(prefix, CancellationToken.None);

        ListObjectsV2Response left = await client.ListObjectsV2Async(new ListObjectsV2Request { BucketName = bucket, Prefix = prefix });
        Assert.True(left.S3Objects is null || left.S3Objects.Count == 0);
    }
}
