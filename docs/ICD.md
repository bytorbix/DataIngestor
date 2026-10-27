
# Marco Polo ICD

The UDP telemetry packet between the Telemetry Simulator (or a real UAV) and the TelemetryDevice. There is
**one ICD**: its fields are named after **MISB ST 0601**, so a dataset's `telemetry.csv`, the ICD and the
JSON that reaches the DataIngestor all use the same names. Builds on the Marco Polo pipeline design.

Source of truth: `Icd/MissionMapTable.json`, identical in UAVTelemetrySimulator and TelemetryDevice.

## 📦 Packet: 70 bytes, little-endian

| Byte | Bits | Field | Type | Unit / range | MISB tag |
| --- | --- | --- | --- | --- | --- |
| 0 | 8 | `sync_1` | uint8 | = 176 | — |
| 1 | 8 | `sync_2` | uint8 | = 59 | — |
| 2 | 8 | `sync_3` | uint8 | = 79 | — |
| 3 | 16 | `Tail number` | uint16 | 0..65000 | — |
| 5 | 4 (low) | `correlator` | bits | optional groups filled (none yet: always 0) | — |
| 5 | 4 (high) | `zero space` | bits | = 0 | — |
| 6 | 64 | `PrecisionTimeStamp` | uint64 | µs since 1970-01-01 UTC | 2 |
| 14 | 32 | `PlatformHeadingAngle` | float32 | deg, 0..360 | 5 |
| 18 | 32 | `PlatformPitchAngle` | float32 | deg, -90..90 | 6 |
| 22 | 32 | `PlatformRollAngle` | float32 | deg, -180..180 | 7 |
| 26 | 64 | `SensorLatitude` | float64 | deg, -90..90 | 13 |
| 34 | 64 | `SensorLongitude` | float64 | deg, -180..180 | 14 |
| 42 | 32 | `SensorTrueAltitude` | float32 | m above MSL, -900..19000 | 15 |
| 46 | 32 | `SensorEllipsoidHeight` | float32 | m above WGS-84, -900..19000 | 75 |
| 50 | 32 | `SensorHorizontalFieldOfView` | float32 | deg, 0..180 | 16 |
| 54 | 32 | `SensorVerticalFieldOfView` | float32 | deg, 0..180 | 17 |
| 58 | 32 | `SensorRelativeAzimuthAngle` | float32 | deg, 0..360 | 18 |
| 62 | 32 | `SensorRelativeElevationAngle` | float32 | deg, -180..180 | 19 |
| 66 | 32 | `SensorRelativeRollAngle` | float32 | deg, 0..360 | 20 |

- **Every field after the header is MISB ST 0601**; only the framing (`sync_*`, `Tail number`, correlator) isn't.
- **Correlator:** every field is in every packet, so it is always 0. It stays in the layout for future
  optional groups; an unfilled group's bytes would be zeros, and the size never changes.
- **Values are engineering units** (no MISB scaled integers). Lat/lon are doubles: float32 was ~1 m at
  these longitudes.
- **Out-of-range values are clamped** by the simulator's encoder.

## ✅ Checks the TelemetryDevice makes

1. `sync_1..3` match.
2. The packet is exactly 70 bytes (the size `MissionMapTable.json` adds up to). A sender built from a
   different table is dropped with a log saying so, instead of decoding as garbage.
3. `Tail number` is in range and registered.

## ⏱️ Time

- Frames and telemetry are matched **only by UTC**: DataIngestor syncs on `PrecisionTimeStamp / 1000` (ms).
- On replay the simulator invents that UTC: `PrecisionTimeStamp` = run `startAt` + the row's position in the
  video. Wowza stamps the video the same way (`startAt` + its position), so both line up.
- The row's position comes from the CSV's `pts_time` column (seconds since the video's first frame). It is
  **simulator input only**, never sent: a real UAV stamps UTC itself. Without a `pts_time` column, the first
  row is taken as the video's first frame.
- The whole CSV is always replayed from its first row (no start index): datasets come prepared and trimmed
  to their video in `Datasets/Flights`.

## 🧩 How each service uses it

| Service | Uses |
| --- | --- |
| Telemetry Simulator | `MisbMapping.json` (CSV column = ICD field) + a `Flights/<name>/telemetry.csv`; rejects an upload whose CSV lacks a mapped column |
| TelemetryDevice | decodes all fields to JSON, adds `Timestamp` (packet capture time), Kafka key = `Tail number` |
| DataIngestor | strips framing (`sync_*`, `correlator`, `zero space`, `Timestamp`), syncs on `PrecisionTimeStamp`, blends the six angles the short way round |
| FlightOrchestrator | requires the mapping to map `PrecisionTimeStamp` |
| Algorithm pipeline | position = lat/lon/ellipsoid height; camera = platform angles · sensor relative angles; intrinsics = FOV + video size |

## 📜 History

Before 2026-10-27 the repos had drifted onto two different layouts with no way to tell them apart:
90 bytes (`time` as 4-byte time-of-day ms + `utc_time_us`; simulator `master`, TD `main`) and 86 bytes
(`time` as 8-byte Unix ms; simulator `time-sync`, TD `fix-utc-time-field`). A sender on one and a receiver
on the other decoded every field after byte 6 shifted by 4 bytes. This ICD replaces both (branch `icd-v3`
in UAVTelemetrySimulator, TelemetryDevice, DataIngestor, FlightOrchestrator).

## ❓ Open

- DJI field of view is assumed (spec sheets), see `Flights/README.md`.
- Correlator groups have no "hold last value" on the receiving side; fine while no field uses one.
- DataIngestor's `tools/sync_accuracy.py` read `pts_time` from the packet and needs adapting.
