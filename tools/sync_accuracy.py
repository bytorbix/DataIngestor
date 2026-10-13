"""
Measures how accurately the DataIngestor paired video frames with telemetry.

Wowza doesn't re-encode the video, so every frame in the saved HLS segments decodes to exactly the
same picture as a frame in the source .ts file. Matching them by a hash of the decoded picture tells
us the frame's ORIGINAL pts in the recording. The telemetry attached to that frame carries the
recording's pts_time, so:

    error = telemetry.pts_time - original frame pts

is the real sync error (0 = the frame got the telemetry recorded at the same moment).

Usage:
    python sync_accuracy.py <source video.ts> <output/42.jsonl> <output/segments/42> [--csv errors.csv]

Needs ffmpeg on PATH and the DataIngestor run with Hls:SegmentArchiveDirectory set.
"""

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from fractions import Fraction


def frame_hashes(path):
    """[(pts_seconds, md5 of decoded picture)] in presentation order, with original timestamps kept."""
    out = subprocess.run(
        # -enc_time_base demux keeps the stream's own 1/90000 clock; otherwise pts get rounded to whole frames
        ["ffmpeg", "-v", "error", "-copyts", "-i", path, "-map", "0:v:0", "-fps_mode", "passthrough", "-enc_time_base", "demux", "-f", "framemd5", "-"],
        capture_output=True, text=True, check=True).stdout

    time_base = None
    frames = []
    for line in out.splitlines():
        if line.startswith("#tb 0:"):
            time_base = Fraction(line.split(":", 1)[1].strip())
        elif line and not line.startswith("#"):
            fields = [f.strip() for f in line.split(",")]
            frames.append((float(int(fields[2]) * time_base), fields[5]))
    return frames


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source")
    parser.add_argument("jsonl")
    parser.add_argument("segments_dir")
    parser.add_argument("--csv", help="write per-frame errors here (for charting)")
    args = parser.parse_args()

    print("Hashing source video...", file=sys.stderr)
    source_pts_by_hash = defaultdict(list)
    for pts, digest in frame_hashes(args.source):
        source_pts_by_hash[digest].append(pts)

    records = [json.loads(line) for line in open(args.jsonl, encoding="utf-8") if line.strip()]
    by_segment = defaultdict(list)
    for record in records:
        by_segment[os.path.basename(record["frame"]["segmentUrl"])].append(record)

    rows = []  # (videoUtcMs, segment, frame pts, original pts, telemetry pts_time, method)
    missing_segments, unmatched_frames, no_telemetry = [], 0, 0

    for segment, segment_records in by_segment.items():
        segment_path = os.path.join(args.segments_dir, segment)
        if not os.path.exists(segment_path):
            missing_segments.append(segment)
            continue

        print(f"Matching {segment}...", file=sys.stderr)
        segment_frames = frame_hashes(segment_path)

        # the whole segment is shifted by one constant offset from the source; vote on it so that
        # frames which don't decode identically (e.g. before the first keyframe) can't mislead us
        votes = Counter()
        exact_offsets = defaultdict(list)
        for pts, digest in segment_frames:
            for source_pts in source_pts_by_hash.get(digest, []):
                key = round(pts - source_pts, 3)
                votes[key] += 1
                exact_offsets[key].append(pts - source_pts)
        if not votes:
            unmatched_frames += len(segment_records)
            continue
        offset = statistics.mean(exact_offsets[votes.most_common(1)[0][0]])
        matched_hashes = {round(pts, 3): digest for pts, digest in segment_frames}

        for record in segment_records:
            frame_pts = record["frame"]["ptsTime"]
            original_pts = frame_pts - offset
            digest = matched_hashes.get(round(frame_pts, 3))
            if digest is None or not any(abs(p - original_pts) < 0.002 for p in source_pts_by_hash.get(digest, [])):
                unmatched_frames += 1
                continue

            telemetry = record.get("telemetry")
            if not telemetry or "pts_time" not in telemetry:
                no_telemetry += 1
                continue

            rows.append((record["videoUtcMs"], segment, frame_pts, original_pts, telemetry["pts_time"], record["sync"]["method"]))

    rows.sort()
    errors_ms = [(tel_pts - original_pts) * 1000 for _, _, _, original_pts, tel_pts, _ in rows]

    print()
    print(f"frames in jsonl:            {len(records)}")
    print(f"matched to source + telem:  {len(rows)}")
    print(f"frames without telemetry:   {no_telemetry}")
    print(f"frames not matched:         {unmatched_frames}")
    if missing_segments:
        print(f"segments not archived:      {len(missing_segments)} ({', '.join(missing_segments[:5])}{'...' if len(missing_segments) > 5 else ''})")
    if not rows:
        return

    abs_errors = [abs(e) for e in errors_ms]
    print()
    print("error = telemetry pts_time - original frame pts  (positive: telemetry is from later in the recording)")
    print(f"  mean   {statistics.mean(errors_ms):9.1f} ms")
    print(f"  median {statistics.median(errors_ms):9.1f} ms")
    print(f"  stdev  {statistics.pstdev(errors_ms):9.1f} ms")
    print(f"  |err| p95 {percentile(abs_errors, 95):6.1f} ms   max {max(abs_errors):.1f} ms")
    print(f"  within one frame (33 ms): {sum(a <= 33.4 for a in abs_errors) / len(abs_errors):.1%}")

    print()
    print("over time (10 s buckets):")
    start = rows[0][0]
    buckets = defaultdict(list)
    for (utc, *_), error in zip(rows, errors_ms):
        buckets[int((utc - start) / 10000)].append(error)
    for bucket, values in sorted(buckets.items()):
        print(f"  t={bucket * 10:4d}s  n={len(values):4d}  mean {statistics.mean(values):9.1f} ms  min {min(values):9.1f}  max {max(values):9.1f}")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["video_utc_ms", "segment", "frame_pts", "original_pts", "telemetry_pts_time", "error_ms", "method"])
            for row, error in zip(rows, errors_ms):
                writer.writerow([*row[:5], round(error, 3), row[5]])
        print(f"\nper-frame errors written to {args.csv}")


if __name__ == "__main__":
    main()
