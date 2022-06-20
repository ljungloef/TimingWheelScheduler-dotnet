(*

   Copyright 2022 The TimingWheelScheduler Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*)

namespace TimingWheelScheduler

open System
open System.Diagnostics
open Microsoft.FSharp.Data.UnitSystems.SI.UnitSymbols

/// Unit for ticks in the timestamp quantity
[<Measure>]
type TimestampTick

/// Unit for ticks in the timespan quantity
[<Measure>]
type TimeSpanTick

module TimeSpan =

  /// Create a new timespan initialized with `seconds`.
  let inline fromSeconds (seconds: float<s>) =
    seconds |> float |> TimeSpan.FromSeconds

  let inline ticks (timeSpan: TimeSpan) =
    timeSpan.Ticks * 1L<TimeSpanTick>

module Timestamp =

  module Ratios =

    /// The number of `TimeSpanTick`s in 1s.
    let timeSpanTickPerSecond = TimeSpan.TicksPerSecond * 1L<TimeSpanTick/s>

    /// The number of `TimestampTick`s in 1s.
    let timestampTicksPerSecond = Stopwatch.Frequency * 1L<TimestampTick/s>

    /// The number of `TimestampTick` that goes in 1 `TimeSpanTick`.
    let timestampTicksPerTimeSpanTicks = timestampTicksPerSecond / timeSpanTickPerSecond

  /// Convert the `timeSpan` to a `Timestamp`
  let inline fromTimeSpan (ts: TimeSpan) =
    (TimeSpan.ticks ts) * Ratios.timestampTicksPerTimeSpanTicks

  /// Convert the `timestamp` to a `TimeSpan`
  let inline toTimeSpan (ticks: int64<TimestampTick>) =
    (float ticks / float Ratios.timestampTicksPerSecond)
    |> TimeSpan.FromSeconds

  /// Gets the current timestamp
  let inline current () =
    Stopwatch.GetTimestamp() * 1L<TimestampTick>