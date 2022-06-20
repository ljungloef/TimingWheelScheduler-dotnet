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

namespace TimingWheelScheduler.Benchmarks.HashedTimingWheel

open BenchmarkDotNet.Attributes
open TimingWheelScheduler
open System
open System.Threading
open System.Diagnostics
open BenchmarkDotNet.Configs
open BenchmarkDotNet.Jobs
open BenchmarkDotNet.Environments
open Microsoft.Diagnostics.NETCore.Client
open Microsoft.Diagnostics.Tracing.Parsers
open System.Diagnostics.Tracing
open BenchmarkDotNet.Diagnosers

type HashedTimingWheelBenchmarkConfig() as this =
  inherit ManualConfig()

  do
    let providers =
      [| EventPipeProvider(
           ClrTraceEventParser.ProviderName,
           EventLevel.Verbose,
           int64 (
             ClrTraceEventParser.Keywords.Exception
             ||| ClrTraceEventParser.Keywords.GC
             ||| ClrTraceEventParser.Keywords.Jit
             ||| ClrTraceEventParser.Keywords.JitTracing
             ||| ClrTraceEventParser.Keywords.Loader
             ||| ClrTraceEventParser.Keywords.NGen
           )
         )
         EventPipeProvider("TimingEventSource", EventLevel.Verbose, Int64.MaxValue) |]

    this.AddJob(Job.ShortRun.WithRuntime(CoreRuntime.Core60))
    |> ignore

    this.AddDiagnoser(EventPipeProfiler(providers = providers))
    |> ignore

[<EventSource(Name = "TimingEventSource")>]
type TimingEventSource() as this =
  inherit EventSource()

  let mutable timingCounter =
    new EventCounter("timing", this, DisplayName = "Offset from optimal timing", DisplayUnits = "ms")

  member this.Add(timing: float) =
    this.WriteEvent(1, timing)
    timingCounter.WriteMetric(timing)

  override __.Dispose(disposing: bool) =
    if not (isNull timingCounter) then
      timingCounter.Dispose()
      timingCounter <- Unchecked.defaultof<EventCounter>

    base.Dispose(disposing)


[<Config(typeof<HashedTimingWheelBenchmarkConfig>)>]
type HashedTimingWheelBenchmark() =

  let mutable scheduler = Unchecked.defaultof<IScheduler>
  let mutable cancellationTokenSource = Unchecked.defaultof<CancellationTokenSource>
  let mutable eventSource = Unchecked.defaultof<TimingEventSource>
  let mutable limit = 0

  [<Params(100)>]
  member val public BucketSize = 0 with get, set

  [<Params(100)>]
  member val public Buckets = 0 with get, set

  [<Params(10)>]
  member val public TickIntervalMs = 0 with get, set

  [<IterationSetup>]
  member self.IterationSetup() =
    cancellationTokenSource <- new CancellationTokenSource()
    eventSource <- new TimingEventSource()
    limit <- self.BucketSize * self.Buckets

    scheduler <-
      TimingWheelScheduler.create (TimeSpan.FromMilliseconds(self.TickIntervalMs)) 1024
      |> Scheduler.start cancellationTokenSource.Token

  [<IterationCleanup>]
  member __.IterationCleanup() =
    scheduler.Dispose()
    cancellationTokenSource.Dispose()
    eventSource.Dispose()

  [<Benchmark>]
  member self.Run() =
    let mutable callbackCount = 0

    let sw = Stopwatch()
    sw.Start()

    use waitHandle = new ManualResetEventSlim(false)

    let callback (state: obj) =
      let actual = sw.ElapsedTicks * 1L<TimestampTick>
      let expected = (state :?> int64) * 1L<TimestampTick>

      let diff =
        ((actual - expected) |> Timestamp.toTimeSpan)
          .TotalMilliseconds

      eventSource.Add(diff)

      let callbackCount = Interlocked.Add(&callbackCount, 1)

      if callbackCount >= limit && waitHandle.IsSet = false then
        waitHandle.Set()

    for i in 1 .. self.Buckets do
      for _ in 1 .. self.BucketSize do
        let delay = TimeSpan.FromMilliseconds(10.0 * float i)
        let deadline = delay |> Timestamp.fromTimeSpan

        scheduler.Schedule(RunOnce delay, deadline, callback)
        |> ignore

    waitHandle.Wait()