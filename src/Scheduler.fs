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

/// A `IScheduler` is a mechanism for scheduling callbacks to run after a certain amount of time.
type IScheduler =

  inherit IDisposable

  /// The `Completion` task will be completed once the scheduler stops.
  /// Can be used as `await scheduler.Completion`, as an equivalent to a `RunUntilStopped()` method.
  abstract member Completion: Task

  /// Schedule a new callback that will run when the `schedule` expires
  abstract member Schedule: schedule: Schedule * obj * Callback -> ITimer

/// An `ITimer` is a handle of a scheduled timer task. The `ITimer` can be used to request the
/// corresponding timer task to be cancelled.
///
/// A timer task marked as cancel, MAY be cancelled if the per tick bookkeeping for that particular timer task's deadline tick has not run.
and ITimer =

  /// Request cancellation of the referenced timer task.
  abstract member Cancel: unit -> unit

/// Available options for scheduling a callback
and [<Struct>] Schedule =

  /// Run once after the delay, represented as a `TimeSpan`
  | RunOnce of runOnce: TimeSpan

  /// Run once after the dalay, represented as a `TimeSpan`, and reschedule for the same delay after the callback completes.
  | RunRepeateadly of runRepeateadly: TimeSpan

/// The callback that should be run once the schedule is met.
and Callback = obj -> unit

module Scheduler =

  open System.Threading

  /// Start a new scheduler. The lifetime of the returned scheduer is controlled by the given `CancellationToken`
  let inline start ct (factory: CancellationToken -> IScheduler) =
    ct |> factory