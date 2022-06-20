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

namespace TimingWheelScheduler.Samples.Comparison

open Plotly.NET

open TimingWheelScheduler

open System
open System.Diagnostics
open System.Threading
open System.Threading.Channels

module Comparison =

  let inline readChannel (reader: ChannelReader<obj * int64<TimestampTick>>) max =
    seq {
      for _ = 1 to max do
        let success, (expected, actual) = reader.TryRead()

        if success then
          let expected = (expected :?> int64) * 1L<TimestampTick>

          let diff =
            ((actual - expected) |> Timestamp.toTimeSpan)
              .TotalMilliseconds

          yield diff
        else
          failwith "Should not happen"
    }

  let inline run name ([<InlineIfLambda>] schedule) rounds =
    let total = rounds * rounds

    let mutable callbackCount = 0

    let channel =
      Channel.CreateBounded(BoundedChannelOptions(total, SingleReader = true, SingleWriter = true))

    use waitHandle = new ManualResetEventSlim(false)

    let sw = Stopwatch()
    sw.Start()

    let startDelay = TimeSpan.FromMilliseconds(500)

    let callback state =
      let written = channel.Writer.TryWrite(state, (sw.ElapsedTicks * 1L<TimestampTick>))

      if not written then
        raise (Exception("Channel full"))

      let callbackCount = Interlocked.Add(&callbackCount, 1)

      if callbackCount % 100_000 = 0 then
        printfn "%i/%i" callbackCount total

      if callbackCount >= total && waitHandle.IsSet = false then
        waitHandle.Set()

    for i in 1..rounds do
      for _ in 1..rounds do
        let delay =
          startDelay
          + TimeSpan.FromMilliseconds(10.0 * float i)

        let deadline = delay |> Timestamp.fromTimeSpan
        schedule delay deadline callback

    waitHandle.Wait()

    sw.Stop()

    let diffs = readChannel channel.Reader total |> Seq.map id

    Chart.Histogram(diffs, NBinsX = 100)
    |> Chart.withTitle $"{name} | {total} occurences"
    |> Chart.withYAxisStyle (TitleText = "Frequency")
    |> Chart.withXAxisStyle (TitleText = "Difference to expiry time (ms)")
    |> Chart.show

module DotNettyTimer =

  open DotNetty.Common.Utilities

  type ActionTimerTaskWithState(state, callback) =

    interface ITimerTask with

      override __.Run(_) = callback state

  let inline run rounds size interval =

    let timer = new HashedWheelTimer(interval, size, 0)

    Comparison.run
      "DotNetty"
      (fun delay deadline callback ->
        timer.NewTimeout(ActionTimerTaskWithState(deadline, callback), delay)
        |> ignore)
      rounds

    { new IDisposable with
        member __.Dispose() = () }

module AkkaScheduler =

  open Akka.Actor
  open Akka.Dispatch
  open Akka.Event
  open Akka.Configuration

  let inline run rounds size interval =

    let config =
      ConfigurationFactory.ParseString(
        sprintf
          @"
          akka{
            scheduler{
              ticks-per-wheel = %i
              tick-duration = %s
            }
          }"
          size
          interval
      )

    let scheduler = new HashedWheelTimerScheduler(config, NoLogger.Instance)

    Comparison.run
      "Akka.NET"
      (fun delay deadline callback ->
        scheduler.ScheduleOnce(delay, ActionWithStateRunnable(callback, deadline))
        |> ignore)
      rounds

    scheduler :> IDisposable

module TimingWheelScheduler =

  let inline run rounds size interval ct =
    let scheduler =
      TimingWheelScheduler.create interval size
      |> Scheduler.start ct

    Comparison.run
      "tws"
      (fun delay deadline callback ->
        scheduler.Schedule(RunOnce delay, deadline, callback)
        |> ignore)
      rounds

    scheduler: IDisposable

module Program =

  let inline runTarget rounds (target: string) ct =
    match target.ToLowerInvariant() with
    | "akka" -> AkkaScheduler.run rounds 2048 "10ms"
    | "dotnetty" -> DotNettyTimer.run rounds 2048 (TimeSpan.FromMilliseconds(10))
    | _ -> TimingWheelScheduler.run rounds 2048 (TimeSpan.FromMilliseconds(10)) ct

  let inline run rounds ct (args: string list) =
    match args with
    | [ target ]
    | target :: _ -> runTarget rounds target ct
    | _ -> runTarget rounds "tws" ct

  [<EntryPoint>]
  let main args =
    try
      use ct = new CancellationTokenSource()

      Console.CancelKeyPress
      |> Event.add (fun _ -> ct.Cancel())

      use _ = args |> List.ofArray |> run 300 ct.Token

      0
    with
    | e -> raise (Exception("Error when running accuracy test. See inner exception for details.", e))