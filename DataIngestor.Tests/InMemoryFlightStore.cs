using DataIngestor.Storage;

// stands in for MinIO so tests can see exactly which keys were written
public class InMemoryFlightStore : IFlightStore
{
    public Dictionary<string, (byte[] Content, string ContentType)> Objects { get; } = new();

    // keys whose upload throws, to stand in for MinIO being unreachable
    public HashSet<string> FailingKeys { get; } = new();

    // every put and delete in order, so tests can check what reached the archive first
    public List<string> Operations { get; } = new();

    public async Task PutAsync(string key, Stream content, string contentType, CancellationToken cancellationToken)
    {
        if (FailingKeys.Contains(key))
            throw new HttpRequestException($"simulated failure for {key}");

        Operations.Add($"put {key}");
        using MemoryStream copy = new();
        await content.CopyToAsync(copy, cancellationToken);
        Objects[key] = (copy.ToArray(), contentType);
    }

    public Task DeletePrefixAsync(string prefix, CancellationToken cancellationToken)
    {
        Operations.Add($"delete {prefix}");
        foreach (string key in Objects.Keys.Where(k => k.StartsWith(prefix, StringComparison.Ordinal)).ToList())
            Objects.Remove(key);
        return Task.CompletedTask;
    }

    public string Text(string key) => System.Text.Encoding.UTF8.GetString(Objects[key].Content);
}
