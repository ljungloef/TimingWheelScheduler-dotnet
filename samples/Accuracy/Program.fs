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

namespace TimingWheelScheduler.Samples.Accuracy

open Plotly.NET

open TimingWheelScheduler

open System
open System.Threading
open System.Threading.Channels

module Accuracy =

  let inline readChannel (reader: ChannelReader<obj * DateTimeOffset>) max =
    seq {
      for _ = 1 to max do
        let success, (expected, actual) = reader.TryRead()

        if success then
          let expected = expected :?> DateTimeOffset
          let diff = (actual - expected).TotalMilliseconds
          yield diff
        else
          failwith "Should not happen"
    }

  let run () =
    let rounds = 100
    let total = rounds * rounds

    let mutable callbackCount = 0

    let channel =
      Channel.CreateBounded(BoundedChannelOptions(total, SingleReader = true, SingleWriter = true))

    use waitHandle = new ManualResetEventSlim(false)
    use ct = new CancellationTokenSource()

    use timer =
      TimingWheelScheduler.create (TimeSpan.FromMilliseconds(5)) 2048
      |> Scheduler.start ct.Token

    let time = DateTimeOffset.UtcNow
    let startDelay = TimeSpan.FromMilliseconds(500)

    let callback state =
      if not (channel.Writer.TryWrite(state, DateTimeOffset.UtcNow)) then
        raise (Exception("Channel full"))

      let callbackCount = Interlocked.Add(&callbackCount, 1)

      if callbackCount % 50_000 = 0 then
        printfn "%i/%i" callbackCount total

      if callbackCount >= total && waitHandle.IsSet = false then
        waitHandle.Set()

    for i in 1..rounds do
      for _ in 1..rounds do
        let delay =
          startDelay
          + TimeSpan.FromMilliseconds(10.0 * float i)

        let deadline = time.Add(delay)

        timer.Schedule(RunOnce delay, deadline, callback)
        |> ignore

    waitHandle.Wait()

    let diffs =
      readChannel channel.Reader total
      |> Seq.map id

    Chart.Histogram(diffs, NBinsX = 100)
    |> Chart.withYAxisStyle (TitleText = "Frequency")
    |> Chart.withXAxisStyle (TitleText = "Difference to expiry time (ms)")
    |> Chart.show

module Program =

  [<EntryPoint>]
  let main args =
    try
      Accuracy.run ()
      0
    with
    | e -> raise (Exception("Error when running accuracy test. See inner exception for details.", e))