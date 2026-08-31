namespace DataIngestor.Channels
{
    using ThreadingChannel = System.Threading.Channels.Channel;
    public class Channel
    {
        public required string TailNumber { get; init; }

        public System.Threading.Channels.Channel<TelemetryRecord> TelemetryChannel { get; }
            = ThreadingChannel.CreateUnbounded<TelemetryRecord>();

        public System.Threading.Channels.Channel<FrameRecord> FrameChannel { get; }
            = ThreadingChannel.CreateUnbounded<FrameRecord>();

    }
}
