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

namespace TimingWheelScheduler.Benchmarks.Scheduling

open BenchmarkDotNet.Attributes
open TimingWheelScheduler
open System
open System.Threading


[<SimpleJob; MemoryDiagnoser>]
type SchedulingBenchmark() =

  let mutable scheduler = Unchecked.defaultof<IScheduler>
  let mutable cancellationTokenSource =  Unchecked.defaultof<CancellationTokenSource>

  [<Params(100_000, 500_000)>]//, 1_000_000)>]
  member val public Count = 0 with get, set

  [<Params(5)>]
  member val public TickIntervalMs = 0 with get, set

  [<IterationSetup>]
  member self.IterationSetup() =
    cancellationTokenSource <- new CancellationTokenSource()
    scheduler <-
      TimingWheelScheduler.create (TimeSpan.FromMilliseconds(self.TickIntervalMs)) 1024
      |> Scheduler.start cancellationTokenSource.Token

  [<IterationCleanup>]
  member __.IterationCleanup() =
    scheduler.Dispose()
    cancellationTokenSource.Dispose()

  [<Benchmark>]
  member self.Run() =
    use waitHandle = new ManualResetEventSlim(false)
    let last = self.Count
    let tickInterval = TimeSpan.FromMilliseconds(self.TickIntervalMs)
    let param = obj()
    let mutable counter = 0

    let callback _ =
      let counter = Interlocked.Increment(&counter)
      if counter = last then
        waitHandle.Set()

    for _ in 1 .. self.Count do
      scheduler.Schedule(RunOnce tickInterval, param, callback) |> ignore

    waitHandle.Wait()