namespace DataIngestor.Storage
{
    // where a flight's objects land (flight.json, chunks, segments); the flight rules live in FlightOutput, not here
    public interface IFlightStore
    {
        // uploads one whole object; an existing key is replaced
        Task PutAsync(string key, Stream content, string contentType, CancellationToken cancellationToken);

        // removes every object under the prefix (a replayed clip replaces its earlier flight)
        Task DeletePrefixAsync(string prefix, CancellationToken cancellationToken);
    }
}
