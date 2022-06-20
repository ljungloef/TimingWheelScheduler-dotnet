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
open System.Threading.Tasks
open System.Threading
open System.Threading.Channels

module Common =

  /// Represents the state of a timer task, being part of a timing wheel
  type TimerTask
    (
      registerTime: int64<TimestampTick>,
      deadline: int64,
      delay: int64<TimestampTick>,
      isRecurring: bool,
      state: obj,
      callback: Callback
    ) =

    /// At what wheel tick the timer task expires
    member val Deadline = deadline with get, set

    /// The time when the timer task was requested to be scheduled
    member val RegisterTime = registerTime

    /// The current status of the timer task.
    member val Mark = TimerTaskMark.Waiting with get, set

    /// Indicator whether the owner has requested the task to be cancelled
    member val CancellationRequested = false with get, set

    /// The state the owner set when scheduling the task. Only kept for passing to the callback
    member val State = state

    /// The callback that should run when the timer task expires
    member val Callback = callback

    /// The timer task's schedule interval
    member val Delay = delay

    /// The timer task's schedule interval
    member val IsRecurring = isRecurring

  /// The different states a timer task can be
  and TimerTaskMark =

    /// `NotSet` is used before the timer task has been scheduled in a wheel
    | NotSet = 0

    /// The timer task has been scheduled and is waiting to expire
    | Waiting = 1

    /// The timer task has expired, the callback ran successfully, and is now waiting to be collected
    | FiredSuccessfully = 2

    /// The timer task has expired, the callback ran unsuccessfully, and is now waiting to be collected
    | Failed = 3

    /// The bookkeeper found the cancellation request before the timer task expired, and is now waiting to be collected
    | Cancelled = 4

  /// The state and definition of a single timing wheel
  type TimingWheel
    (
      wheelTick: int64,
      startTime: int64<TimestampTick>,
      tickInterval: int64<TimestampTick>,
      length: int64,
      buckets: Bucket array
    ) =

    /// The current wheel tick position
    member val WheelTick = wheelTick with get, set

    /// The time when the wheel was started
    member val StartTime = startTime

    /// The amoutn of time each tick represent
    member val TickInterval = tickInterval

    /// How many buckets the timing wheel consists of
    member val Length = length

    /// The timing wheel's buckets
    member val Buckets = buckets

  and Bucket(values: TimerTask list) =

    member val Lock = obj ()
    member val Values = values with get, set

  /// Used internally to communicate when the per tick bookkeeping round complete for the given bucket (`idx`) and is ready to be collected
  [<Struct>]
  type CollectCommand =

    /// At what timestamp the collector ran
    val RelativeTimestamp: int64<TimestampTick>

    /// The index of the bucket than is ready to be collected
    val Idx: int

    new(relativeTimestamp, idx) =
      { RelativeTimestamp = relativeTimestamp
        Idx = idx }

  module Helpers =

    let inline normPowerOf2 (v: int) =
      let mutable n = 1

      while (n < v) do
        n <- n <<< 1

      n

module Collector =

  open Common

  let inline updateBucket (ring: Bucket array) idx ([<InlineIfLambda>] updater) =
    let bucket = ring[idx]

    lock bucket.Lock (fun _ -> bucket.Values <- updater bucket.Values)

    bucket

  let inline appendTimer (ring: Bucket array) idx timerTask =
    updateBucket ring idx (fun values -> timerTask :: values)
    |> ignore

    timerTask

  let inline hash (tick: int64) (mask: int64) = tick % mask |> int

  let inline calculateDeadline (delay: int64<TimestampTick>) (relTo: int64<TimestampTick>) (wheel: TimingWheel) =
    let absDeadline = relTo + delay
    let relToStart = absDeadline - wheel.StartTime

    let deadline =
      Math.Round(float relToStart / float wheel.TickInterval)
      |> int64

    // Minimize the risk to schedule it earlier than the current wheel tick, or they will have to wait a full circle before being detected.
    let deadline =
      if deadline <= wheel.WheelTick then
        wheel.WheelTick + 1L
      else
        deadline

    let idx = hash deadline wheel.Length
    (deadline, idx)

  let inline schedule (wheel: TimingWheel) (timer: TimerTask) =
    // The `Deadline` of an unscheduled timer task is what the wheel position was when the timer task was requested to be scheduled.
    let (deadline, idx) = calculateDeadline timer.Delay timer.RegisterTime wheel

    timer.Mark <- TimerTaskMark.Waiting
    timer.Deadline <- deadline

    appendTimer wheel.Buckets idx timer

  let inline reschedule (timer: TimerTask) (wheel: TimingWheel) lastFireTick =
    let (deadline, idx) = calculateDeadline timer.Delay lastFireTick wheel

    timer.Mark <- TimerTaskMark.Waiting
    timer.Deadline <- deadline

    idx

  let inline checkTimer (timer: TimerTask) wheel relativeTimestamp idx innerResult =
    match timer.Mark with
    | TimerTaskMark.Waiting -> timer :: innerResult // Keep it for a later round
    | TimerTaskMark.Failed
    | TimerTaskMark.FiredSuccessfully when timer.IsRecurring = true ->
      let targetIdx = reschedule timer wheel relativeTimestamp

      if targetIdx = idx then
        timer :: innerResult
      else
        appendTimer wheel.Buckets targetIdx timer
        |> ignore

        innerResult

    // If the timeout has any other mark, it should be removed from the bucket:
    | _ -> innerResult

  let rec collectBucket (bucket: TimerTask list) (wheel: TimingWheel) relativeTimestamp idx result =
    match bucket with
    | [] -> result
    | [ item ] -> checkTimer item wheel relativeTimestamp idx result
    | item :: rest ->
      checkTimer item wheel relativeTimestamp idx result
      |> collectBucket rest wheel relativeTimestamp idx

  let inline collectIdx (wheel: TimingWheel) relativeTimestamp idx =
    updateBucket wheel.Buckets idx (fun existing -> collectBucket existing wheel relativeTimestamp idx [])

module Checker =

  open Common

  let inline mark (timer: TimerTask) marker (results: int) =
    timer.Mark <- marker
    results + 1

  let inline checkTimerTask ([<InlineIfLambda>] whenExpired) currentRound (timer: TimerTask) (results: int) =
    if timer.Mark = TimerTaskMark.NotSet then
      failwith "Bug"

    elif timer.CancellationRequested then

      mark timer TimerTaskMark.Cancelled results

    elif timer.Mark = TimerTaskMark.Waiting
         && currentRound >= timer.Deadline then

      let successful = whenExpired timer

      let marker =
        if successful then
          TimerTaskMark.FiredSuccessfully
        else
          TimerTaskMark.Failed

      mark timer marker results

    else
      results

  let rec checkBucket whenExpired currentRound (bucket: TimerTask list) result =
    match bucket with
    | [] -> result
    | [ timer ] -> checkTimerTask whenExpired currentRound timer result
    | head :: rest ->
      checkTimerTask whenExpired currentRound head result
      |> checkBucket whenExpired currentRound rest

module TaskInvocationStrategies =

  open Common

  let fireAndForget (ct: CancellationToken) (timer: TimerTask) =
    Task.Factory.StartNew(timer.Callback, timer.State, ct, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
    |> ignore

    true

  let syncWait onExn (timer: TimerTask) =
    try
      timer.Callback timer.State
      true
    with
    | e ->
      onExn e
      false

  let inline custom (f: TimerTask -> bool) = f

module WaitStrategies =

  let delayAsync (deadline: int64<TimestampTick>) (ct: CancellationToken) =
    let diff = deadline - (Timestamp.current ())

    if diff <= 0L<TimestampTick> then
      ValueTask()
    else
      let sleepTime = diff |> Timestamp.toTimeSpan
      Task.Delay(sleepTime, ct) |> ValueTask

  let rec spinWait
    (p1Threshold: int64<TimestampTick>)
    (p2Threshold: int64<TimestampTick>)
    (deadline: int64<TimestampTick>)
    (ct: CancellationToken)
    =
    let remaining = deadline - Timestamp.current ()

    if remaining < 0L<TimestampTick> then
      ValueTask()

    elif remaining >= p1Threshold then

      remaining - p2Threshold
      |> Timestamp.toTimeSpan
      |> Thread.Sleep

      spinWait p1Threshold p2Threshold deadline ct
    else
      SpinWait.SpinUntil(fun _ -> deadline <= Timestamp.current ())
      ValueTask()

  let inline defaultSpinWait () =
    let p1 = Timestamp.fromTimeSpan (TimeSpan.FromMilliseconds(16))
    let p2 = Timestamp.fromTimeSpan (TimeSpan.FromMilliseconds(2))
    spinWait p1 p2

module TaskLoop =

  open Common
  open Collector
  open Checker

  let inline waitForWheelTick
    ([<InlineIfLambda>] waiter: int64<TimestampTick> -> CancellationToken -> ValueTask)
    (wheel: TimingWheel)
    (target: int64<TimestampTick>)
    (ct: CancellationToken)
    =
    backgroundTask {
      let current = Timestamp.current ()
      let diff = target - current

      if diff <= 0L<TimestampTick> then
        // no need to wait
        let ticksMissed = float -diff / float wheel.TickInterval

        return
          if ticksMissed < 1. then
            1L
          else
            int64 (Math.Floor(ticksMissed))
      else
        do! waiter target ct
        return 1L
    }

  let inline check (wheel: TimingWheel) (commands: ChannelWriter<_>) (ct: CancellationToken) =
    backgroundTask {
      let callback = TaskInvocationStrategies.fireAndForget CancellationToken.None
      let waiter = WaitStrategies.delayAsync

      while not ct.IsCancellationRequested do
        let nextTick =
          wheel.StartTime
          + (wheel.WheelTick * wheel.TickInterval)

        let! wheelTicks = waitForWheelTick waiter wheel nextTick ct

        for tick = 1L to wheelTicks do
          let idx = hash wheel.WheelTick wheel.Length
          let bucket = wheel.Buckets[idx]

          let timeoutsChanged = checkBucket callback wheel.WheelTick bucket.Values 0

          if timeoutsChanged > 0 then
            let relTime =
              if tick = 1L then
                nextTick
              else
                nextTick + tick * wheel.TickInterval

            let msg = CollectCommand(relTime, idx)

            if not (commands.TryWrite(msg)) then
              do! commands.WriteAsync(msg, ct)

          wheel.WheelTick <- wheel.WheelTick + 1L
    }

  let inline scheduler (wheel: TimingWheel) (reader: ChannelReader<_>) (ct: CancellationToken) =
    backgroundTask {
      let mutable cont = true

      while cont do
        let! hasItems = reader.WaitToReadAsync(ct)
        let mutable read = hasItems

        while read do
          let successful, (timer: TimerTask) = reader.TryRead()
          read <- successful

          if read then
            schedule wheel timer |> ignore

        cont <- hasItems
    }

  let inline collect (wheel: TimingWheel) (reader: ChannelReader<_>) (ct: CancellationToken) =
    backgroundTask {
      let mutable cont = true

      while cont do
        let! hasItems = reader.WaitToReadAsync(ct)
        let mutable read = hasItems

        while read do
          let successful, (command: CollectCommand) = reader.TryRead()
          read <- successful

          if read then
            collectIdx wheel command.RelativeTimestamp command.Idx
            |> ignore

        cont <- hasItems
    }

  let inline longRunning ([<InlineIfLambda>] f: unit -> Task) =
    Task.Factory.StartNew(
      Action (fun _ ->
        let t = f ()
        t.Wait()),
      TaskCreationOptions.LongRunning
    )

  let inline run ([<InlineIfLambda>] f: unit -> Task<'a>) = Task.Run<'a>(Func<Task<'a>>(f))

  let inline completion (a: Task) b c (ct: CancellationToken) =
    backgroundTask {
      try
        let! completedTask = Task.WhenAny(a, b, c)

        if ct.IsCancellationRequested then
          return ()
        else
          // Await so it throws
          return! completedTask
      with
      | :? OperationCanceledException -> return ()
      | e -> raise (Exception("Unexpected exception occured in timer, see inner exception for details.", e))
    }

  let start wheel (schedules: Channel<_>) (collects: Channel<_>) ct =
    let checkTask = (fun () -> check wheel collects.Writer ct) |> run

    let schedulerTask =
      (fun () -> scheduler wheel schedules.Reader ct)
      |> run

    let collectTask =
      (fun () -> collect wheel collects.Reader ct)
      |> run

    completion checkTask schedulerTask collectTask ct

module TimingWheelScheduler =

  open Common

  type TimingWheelScheduler
    (
      channel: ChannelWriter<TimerTask>,
      wheel: TimingWheel,
      completion,
      killSwitch: CancellationTokenSource
    ) =

    let mutable disposed = 0

    let dispose (disposing: bool) =
      if Interlocked.Exchange(&disposed, 1) = 0 then
        killSwitch.Cancel()

        if disposing then killSwitch.Dispose()

    let throwIfDisposed () =
      if disposed > 0 then
        raise (ObjectDisposedException(nameof (Timer)))

    static let asTimer (timer: TimerTask) =
      { new ITimer with
          override __.Cancel() =
            // Do not cancel it right away. The checker will peek CancellationRequested before
            // taking decision on a timeout.
            //
            // If the check identifies that the timeout has been cancelled, it will mark the timeout
            // as cancelled and the collector will remove it eventually.
            timer.CancellationRequested <- true }

    interface IScheduler with

      override __.Completion = completion

      override __.Schedule(schedule, state, callback) =
        throwIfDisposed ()

        let (delay, isRecurring) =
          match schedule with
          | RunOnce ts -> (Timestamp.fromTimeSpan ts, false)
          | RunRepeateadly ts -> (Timestamp.fromTimeSpan ts, true)

        let time = Timestamp.current ()
        let timer = TimerTask(time, 0L, delay, isRecurring, state, callback)

        if not (channel.TryWrite(timer)) then
          let write = channel.WriteAsync(timer).AsTask()

          if not (write.Wait(1_000)) then
            raise (Exception("Channel full"))

        timer |> asTimer

      override self.Dispose() =
        dispose true
        GC.SuppressFinalize(self)

    override __.Finalize() = dispose false

  /// Create a new timing wheel scheduler with the given `tickInterval` and `wheelSize`. Note that the minimum tick interval is 10ms and
  /// the wheelSize will be normalized, and rounded up to the closest power of 2-value.
  ///
  /// The create method returns a factory method that can be passed to `Scheduler.run cancellationToken` to be started.
  let inline create tickInterval wheelSize =
    if tickInterval < TimeSpan.FromMilliseconds(5) then
      invalidArg (nameof tickInterval) "The minimum tick interval is (currently) 5ms"

    let wheelSize = Helpers.normPowerOf2 wheelSize

    fun (ct: CancellationToken) ->
      let killSwitch = CancellationTokenSource.CreateLinkedTokenSource(ct)

      let wheel =
        TimingWheel(
          0L,
          Timestamp.current (),
          Timestamp.fromTimeSpan tickInterval,
          int64 wheelSize,
          Array.init wheelSize (fun _ -> List.empty<TimerTask> |> Bucket)
        )

      let schedules =
        BoundedChannelOptions(
          1_250_000,
          SingleReader = true,
          SingleWriter = true,
          FullMode = BoundedChannelFullMode.Wait
        )
        |> Channel.CreateBounded

      let collects =
        BoundedChannelOptions(
          wheelSize,
          SingleReader = true,
          SingleWriter = true,
          FullMode = BoundedChannelFullMode.Wait
        )
        |> Channel.CreateBounded

      let completion = TaskLoop.start wheel schedules collects killSwitch.Token
      new TimingWheelScheduler(schedules.Writer, wheel, completion, killSwitch) :> IScheduler