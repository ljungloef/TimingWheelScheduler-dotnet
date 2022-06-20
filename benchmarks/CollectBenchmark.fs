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

namespace TimingWheelScheduler.Benchmarks.Collect

open BenchmarkDotNet.Attributes
open TimingWheelScheduler
open System
open TimingWheelScheduler.Common


[<SimpleJob; MemoryDiagnoser>]
type CollectBenchmark() =

  let mutable bucket = []
  let mutable wheel = Unchecked.defaultof<TimingWheel>

  [<Params(100_000, 500_000, 1_000_000)>]
  member val public Count = 0 with get, set

  [<IterationSetup>]
  member self.IterationSetup() =
    wheel <- TimingWheel(0L, Timestamp.current (), Timestamp.fromTimeSpan (TimeSpan.FromMilliseconds(10)), 512L, [||])

    bucket <-
      List.init self.Count (fun i ->
        // Collect 50%
        let mark =
          if i % 2 = 0 then
            TimerTaskMark.Waiting
          else
            TimerTaskMark.FiredSuccessfully

        TimerTask(0L<TimestampTick>, 0L, 1_000L<TimestampTick>, false, obj (), ignore, Mark = mark))

  [<Benchmark>]
  member __.Run() =
    Collector.collectBucket bucket wheel 0L<TimestampTick> 0 []