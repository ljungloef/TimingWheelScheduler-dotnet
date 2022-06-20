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

namespace TimingWheelScheduler.Tests

open Xunit
open FsUnit.Xunit
open TimingWheelScheduler
open TimingWheelScheduler.Common
open FsUnit.CustomMatchers
open System

module HashedWheelTimerTests =

  let defaultDelay = 1_000L<TimestampTick>
  let defaultTickInterval = 1_000L<TimestampTick>
  let defaultStartTIme = 0L<TimestampTick>

  let callback (_: obj) = ()

  let inline createWheel wheelTick (length: int) (timerTasks: TimerTask seq) =
    let wheel =
      TimingWheel(
        wheelTick,
        defaultStartTIme,
        defaultTickInterval,
        int64 length,
        Array.init length (fun _ -> List.empty<TimerTask> |> Bucket)
      )

    for timeout in timerTasks do
      let idx = timeout.Deadline % wheel.Length |> int
      let bucket = wheel.Buckets[idx]
      bucket.Values <- timeout :: bucket.Values

    wheel

  let inline defaultTimerTask id deadline mark =
    TimerTask(0L<TimestampTick>, deadline, defaultDelay, false, id, callback, Mark = mark)

  let inline callbackTimerTask deadline mark callback state =
    TimerTask(0L<TimestampTick>, deadline, defaultDelay, false, state, callback, Mark = mark)

  let inline recurringTimerTask id deadline mark delay =
    TimerTask(0L<TimestampTick>, deadline, delay, true, id, callback, Mark = mark)

  let inline scheduledOnceTimerTask id registerTime deadline delay =
    TimerTask(registerTime, deadline, delay, false, id, callback)

  let inline compareableBucket (bucket: Bucket) =
    bucket.Values
    |> List.map (fun timeout -> timeout.State :?> int)

  let inline compareableWheel (wheel: TimingWheel) =
    wheel.Buckets
    |> Array.map compareableBucket
    |> Array.toList

  let inline allBeEmpty (container: (_ list) list) =
    let len = container |> List.collect id |> List.length
    len |> should equal 0

  module HelpersTests =

    [<Theory>]
    [<InlineData(0, 1)>]
    [<InlineData(1, 1)>]
    [<InlineData(2, 2)>]
    [<InlineData(3, 4)>]
    [<InlineData(5, 8)>]
    [<InlineData(6, 8)>]
    [<InlineData(13, 16)>]
    [<InlineData(256, 256)>]
    [<InlineData(312, 512)>]
    [<InlineData(721, 1024)>]
    let ``nextPowerOf2 should find the next power of 2`` input expected =
      let actual = Helpers.normPowerOf2 input
      actual |> should equal expected

  module WheelTimerChecker =

    let taskInvocation = TaskInvocationStrategies.syncWait ignore

    [<Fact>]
    let ``checkBucket should mark timeout with cancel when timeout is requested to be cancelled`` () =
      let timer = defaultTimerTask 1 2L TimerTaskMark.Waiting
      timer.CancellationRequested <- true

      let bucket = [ timer ]

      let changed = Checker.checkBucket taskInvocation 1L bucket 0

      changed |> should equal 1

      timer.Mark |> should equal TimerTaskMark.Cancelled

    [<Fact>]
    let ``checkBucket should ignore timeout with scheduled for a future run`` () =
      let timer = defaultTimerTask 1 2L TimerTaskMark.Waiting

      let bucket = [ timer ]

      let changed = Checker.checkBucket taskInvocation 1L bucket 0

      changed |> should equal 0
      timer.Mark |> should equal TimerTaskMark.Waiting

    [<Theory>]
    [<InlineData(0UL)>]
    [<InlineData(1UL)>]
    let ``checkBucket should run callback when deadline is reached`` deadline =
      let mutable callbackCalled = false
      let callback _ = callbackCalled <- true

      let timer = callbackTimerTask deadline TimerTaskMark.Waiting callback 1

      let bucket = [ timer ]

      let changed = Checker.checkBucket taskInvocation 1L bucket 0

      changed |> should equal 1

      timer.Mark
      |> should equal TimerTaskMark.FiredSuccessfully

      callbackCalled |> should equal true

    [<Fact>]
    let ``checkBucket should run callback for all timeouts with passed deadlines`` () =
      let mutable callbackCalledFor = []

      let callback (id: obj) =
        callbackCalledFor <- id :?> int :: callbackCalledFor

      let timer1 = callbackTimerTask 1L TimerTaskMark.Waiting callback 1
      let timer2 = callbackTimerTask 2L TimerTaskMark.Waiting callback 2
      let timer3 = callbackTimerTask 1L TimerTaskMark.Waiting callback 3

      let bucket = [ timer1; timer2; timer3 ]

      let changed = Checker.checkBucket taskInvocation 1L bucket 0

      changed |> should equal 2
      callbackCalledFor |> should matchList [ 1; 3 ]

    [<Fact>]
    let ``checkBucket with exception in callback should mark timeout as failed and catch any exception`` () =
      let callback _ = raise (Exception("Error"))

      let timer = callbackTimerTask 1L TimerTaskMark.Waiting callback (obj ())

      let bucket = [ timer ]

      let changed = Checker.checkBucket taskInvocation 1L bucket 0

      changed |> should equal 1
      timer.Mark |> should equal TimerTaskMark.Failed

  module WheelTimerCollector =

    [<Fact>]
    let ``collectBucket should remove all cancelled timeouts`` () =
      let timer = defaultTimerTask 1 1L TimerTaskMark.Cancelled

      let wheel = createWheel 1L 4 [ timer ]

      let result = Collector.collectIdx wheel 0L<TimestampTick> 1

      result |> compareableBucket |> should matchList []
      wheel |> compareableWheel |> allBeEmpty

    [<Fact>]
    let ``collectBucket should keep all waiting timeouts`` () =
      let timer1 = defaultTimerTask 1 1L TimerTaskMark.Waiting
      let timer2 = defaultTimerTask 2 2L TimerTaskMark.Waiting

      let wheel = createWheel 1L 4 [ timer1; timer2 ]

      let result = Collector.collectIdx wheel 0L<TimestampTick> 1

      result
      |> compareableBucket
      |> should matchList [ 1 ]

      wheel
      |> compareableWheel
      |> should matchList [ []; [ 1 ]; [ 2 ]; [] ]

    [<Theory>]
    [<InlineData(TimerTaskMark.FiredSuccessfully)>]
    [<InlineData(TimerTaskMark.Failed)>]
    let ``collectBucket should remove all completed timeouts without recurring schedule`` mark =
      let timer = defaultTimerTask 1 1L mark
      let wheel = createWheel 1L 4 [ timer ]

      let result = Collector.collectIdx wheel 0L<TimestampTick> 1

      result |> compareableBucket |> should matchList []
      wheel |> compareableWheel |> allBeEmpty

    [<Theory>]
    [<InlineData(TimerTaskMark.FiredSuccessfully)>]
    [<InlineData(TimerTaskMark.Failed)>]
    let ``collectBucket should reschedule all completed timeouts with a recurring schedule`` mark =
      let timer = recurringTimerTask 1 1L mark 5_000L<TimestampTick>

      let wheel = createWheel 1L 4 [ timer ]

      let result = Collector.collectIdx wheel 1_000L<TimestampTick> 1

      result |> compareableBucket |> should matchList []

      wheel
      |> compareableWheel
      |> should matchList [ []; []; [ 1 ]; [] ] // Started at [1] and should be rescheduled at 5 ticks away

    [<Fact>]
    let ``collectBucket should reschedule into last bucket correctly`` () =
      let timer =
        recurringTimerTask 1 1L TimerTaskMark.FiredSuccessfully 2_000L<TimestampTick>

      let wheel = createWheel 1L 4 [ timer ]

      let result = Collector.collectIdx wheel 0L<TimestampTick> 1

      result |> compareableBucket |> should matchList []

      wheel
      |> compareableWheel
      |> should matchList [ []; []; []; [ 1 ] ]

    [<Fact>]
    let ``collectBucket should reschedule into first bucket correctly`` () =
      let timer =
        recurringTimerTask 1 1L TimerTaskMark.FiredSuccessfully 3_000L<TimestampTick>

      let wheel = createWheel 1L 4 [ timer ]

      let result = Collector.collectIdx wheel 0L<TimestampTick> 1

      result |> compareableBucket |> should matchList []

      wheel
      |> compareableWheel
      |> should matchList [ [ 1 ]; []; []; [] ]

    [<Fact>]
    let ``schedule should place timeout in the correct bucket`` () =
      let wheel = createWheel 1L 4 []
      let newTimeout = scheduledOnceTimerTask 1 1_000L<TimestampTick> 1L 3_000L<TimestampTick>

      let timeout = Collector.schedule wheel newTimeout

      timeout.Mark |> should equal TimerTaskMark.Waiting
      timeout.Deadline |> should equal 4L

      wheel
      |> compareableWheel
      |> should matchList [ [ 1 ]; []; []; [] ]

    [<Fact>]
    let ``schedule should place timeout in the correct bucket when deadline is multiple rounds away`` () =
      let newTimeout = scheduledOnceTimerTask 1 1_000L<TimestampTick> 1L 9_000L<TimestampTick>
      let wheel = createWheel 1L 4 []

      let timeout = Collector.schedule wheel newTimeout

      timeout.Mark |> should equal TimerTaskMark.Waiting
      timeout.Deadline |> should equal 10L

      wheel
      |> compareableWheel
      |> should matchList [ []; [ ]; [ 1 ]; [] ]