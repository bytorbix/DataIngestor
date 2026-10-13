using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

namespace DataIngestor.Ingestion
{
    // decodes the UTC timestamp Wowza embeds in each segment's ID3 packets
    public static class Id3TimestampDecoder
    {
        private static readonly Regex EpochMillisRegex = new(@"(\d{10,})\|", RegexOptions.Compiled);

        // ffprobe -show_data prints packet bytes as a hex dump, one line per 16 bytes:
        //   "00000000: 4944 3304 0000 0000 003f 5052 4956 0000  ID3......?PRIV.."
        // each line is <offset>: <hex groups>  <ascii preview>; only the hex groups are kept,
        // so that line yields the bytes 49 44 33 04 00 ... 00 (the offset and ascii column are dropped)
        public static byte[] ParseHexDump(string hexDump)
        {
            List<byte> bytes = new();

            foreach (string rawLine in hexDump.Split('\n'))
            {
                string line = rawLine.Trim();
                if (line.Length == 0) continue;

                int colonIndex = line.IndexOf(':');
                if (colonIndex < 0) continue;

                string afterOffset = line[(colonIndex + 1)..];
                int asciiGapIndex = afterOffset.IndexOf("  ", StringComparison.Ordinal);
                string hexPart = asciiGapIndex >= 0 ? afterOffset[..asciiGapIndex] : afterOffset;

                foreach (string group in hexPart.Split(' ', StringSplitOptions.RemoveEmptyEntries))
                    for (int i = 0; i + 1 < group.Length; i += 2)
                        bytes.Add(Convert.ToByte(group.Substring(i, 2), 16));
            }

            return bytes.ToArray();
        }

        public static long? ExtractEpochMillis(byte[] id3Bytes)
        {
            string text = Encoding.Latin1.GetString(id3Bytes);
            Match match = EpochMillisRegex.Match(text);
            return match.Success ? long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) : null;
        }
    }
}
