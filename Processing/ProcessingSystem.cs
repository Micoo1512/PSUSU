using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kolokvijum1.Models;

namespace Kolokvijum1.Processing
{
    public class JobReportEntry
    {
        public JobType Type { get; set; }
        public int DurationMs { get; set; }
        public bool Success { get; set; }
    }

    public class ProcessingSystem : IDisposable
    {
        private readonly int _workerCount;
        private readonly int _maxQueueSize;
        private readonly int _failTimeoutMs;

        // simple list used as priority queue (lower Priority value = higher priority)
        private readonly List<Job> _queue = new List<Job>();
        private readonly object _queueLock = new object();

        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly List<Task> _workers = new List<Task>();

        // idempotency: map job id to task
        private readonly Dictionary<Guid, Task<int>> _executed = new Dictionary<Guid, Task<int>>();
        private readonly Dictionary<Guid, TaskCompletionSource<int>> _tcsMap = new Dictionary<Guid, TaskCompletionSource<int>>();
        private readonly Dictionary<Guid, Job> _allJobs = new Dictionary<Guid, Job>();

        // finished job records for report
        private readonly List<JobReportEntry> _finished = new List<JobReportEntry>();
        private readonly object _finishedLock = new object();

        // events
        public event Action<Job, int>? JobCompleted;
        public event Action<Job, string>? JobFailed;

        // Add optional fail timeout (ms) for tests; default 2000
        public ProcessingSystem(int workerCount, int maxQueueSize, int failTimeoutMs = 2000)
        {
            _workerCount = Math.Max(1, workerCount);
            _maxQueueSize = Math.Max(1, maxQueueSize);
            _failTimeoutMs = Math.Max(1, failTimeoutMs);

            for (int i = 0; i < _workerCount; i++)
            {
                _workers.Add(Task.Run(() => WorkerLoop(_cts.Token)));
            }
        }

        // Submit a job. Throws if queue full. Returns JobHandle with task representing eventual result.
        public JobHandle Submit(Job job)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));

            lock (_queueLock)
            {
                // idempotent: if exists return existing task
                if (_executed.TryGetValue(job.Id, out var existing))
                {
                    return new JobHandle { Id = job.Id, Result = existing };
                }

                if (_queue.Count >= _maxQueueSize)
                {
                    throw new InvalidOperationException("Queue is full");
                }

                // store job
                _allJobs[job.Id] = job;

                var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
                _tcsMap[job.Id] = tcs;
                _executed[job.Id] = tcs.Task;

                // insert keeping priority order (simple insertion)
                int index = _queue.FindIndex(j => j.Priority > job.Priority);
                if (index < 0) _queue.Add(job); else _queue.Insert(index, job);

                Monitor.PulseAll(_queueLock);

                return new JobHandle { Id = job.Id, Result = tcs.Task };
            }
        }

        // Return first n jobs by priority (snapshot)
        public IEnumerable<Job> GetTopJobs(int n)
        {
            lock (_queueLock)
            {
                return _queue.Take(Math.Max(0, n)).Select(j => new Job { Id = j.Id, Type = j.Type, Payload = j.Payload, Priority = j.Priority }).ToList();
            }
        }

        // Return job object by id if known (either queued or seen before)
        public Job? GetJob(Guid id)
        {
            lock (_queueLock)
            {
                if (_allJobs.TryGetValue(id, out var j)) return j;
            }
            return null;
        }

        private async Task WorkerLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                Job? job = null;
                lock (_queueLock)
                {
                    while (_queue.Count == 0 && !token.IsCancellationRequested)
                    {
                        Monitor.Wait(_queueLock, 500);
                    }
                    if (token.IsCancellationRequested) break;
                    if (_queue.Count > 0)
                    {
                        job = _queue[0];
                        _queue.RemoveAt(0);
                    }
                }

                if (job == null) continue;

                TaskCompletionSource<int>? tcs = null;
                lock (_queueLock)
                {
                    _tcsMap.TryGetValue(job.Id, out tcs);
                }

                if (tcs == null)
                {
                    continue; // nothing to complete
                }

                // retry logic: up to 3 attempts
                int attempts = 0;
                bool success = false;
                int result = 0;
                Exception? lastEx = null;
                long totalDurationMs = 0;
                var totalStopwatch = System.Diagnostics.Stopwatch.StartNew();

                while (attempts < 3 && !success)
                {
                    attempts++;
                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        result = await ProcessJob(job).ConfigureAwait(false);
                        sw.Stop();

                        // if took longer than configured timeout -> consider failed attempt
                        if (sw.ElapsedMilliseconds > _failTimeoutMs)
                        {
                            lastEx = new TimeoutException("Job execution exceeded configured timeout");
                            success = false;
                        }
                        else
                        {
                            success = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        lastEx = ex;
                        success = false;
                    }

                    if (!success && attempts < 3)
                    {
                        // small delay before retry
                        await Task.Delay(50).ConfigureAwait(false);
                    }
                }

                totalStopwatch.Stop();
                totalDurationMs = totalStopwatch.ElapsedMilliseconds;

                if (success)
                {
                    // Record finished
                    lock (_finishedLock)
                    {
                        _finished.Add(new JobReportEntry { Type = job.Type, DurationMs = (int)totalDurationMs, Success = true });
                        if (_finished.Count > 1000) _finished.RemoveRange(0, _finished.Count - 1000);
                    }

                    tcs.SetResult(result);
                    JobCompleted?.Invoke(job, result);

                    // cleanup tcs map to avoid holding onto completed TCS unnecessarily
                    lock (_queueLock)
                    {
                        _tcsMap.Remove(job.Id);
                    }
                }
                else
                {
                    // after 3 attempts failed -> ABORT
                    lock (_finishedLock)
                    {
                        _finished.Add(new JobReportEntry { Type = job.Type, DurationMs = (int)totalDurationMs, Success = false });
                        if (_finished.Count > 1000) _finished.RemoveRange(0, _finished.Count - 1000);
                    }

                    // set exception so awaiting clients know it failed
                    if (lastEx == null)
                        tcs.SetException(new Exception("Job failed and aborted"));
                    else
                        tcs.SetException(lastEx);

                    JobFailed?.Invoke(job, "ABORT");

                    // cleanup tcs map to avoid holding onto completed TCS unnecessarily
                    lock (_queueLock)
                    {
                        _tcsMap.Remove(job.Id);
                    }
                }
            }
        }

        // simple processing implementations
        private Task<int> ProcessJob(Job job)
        {
            // run handlers directly on worker thread to avoid extra Task.Run overhead
            return job.Type switch
            {
                JobType.Prime => Task.FromResult(HandlePrime(job.Payload)),
                JobType.IO => Task.FromResult(HandleIO(job.Payload)),
                _ => Task.FromResult(0)
            };
        }

        private int ParseIntFromPayload(string payload, string key)
        {
            if (string.IsNullOrWhiteSpace(payload)) return 0;
            var parts = payload.Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (var p in parts)
            {
                var kv = p.Split(':', StringSplitOptions.RemoveEmptyEntries);
                if (kv.Length != 2) continue;
                if (kv[0].Trim().Equals(key, StringComparison.OrdinalIgnoreCase))
                {
                    var v = kv[1].Trim().Replace("_", string.Empty);
                    if (int.TryParse(v, out var r)) return r;
                }
            }
            return 0;
        }

        private int HandleIO(string payload)
        {
            var delay = ParseIntFromPayload(payload, "delay");
            Thread.Sleep(delay);
            return System.Random.Shared.Next(0, 101);
        }

        private int HandlePrime(string payload)
        {
            var numbers = ParseIntFromPayload(payload, "numbers");
            var threads = ParseIntFromPayload(payload, "threads");
            threads = Math.Clamp(threads, 1, 8);

            if (numbers < 2) return 0;
            int count = 0;
            var po = new ParallelOptions { MaxDegreeOfParallelism = threads };
            Parallel.For(2, numbers + 1, po, i =>
            {
                if (IsPrime(i))
                {
                    Interlocked.Increment(ref count);
                }
            });
            return count;
        }

        private static bool IsPrime(int n)
        {
            if (n <= 1) return false;
            if (n <= 3) return true;
            if (n % 2 == 0) return false;
            var r = (int)Math.Sqrt(n);
            for (int i = 3; i <= r; i += 2)
                if (n % i == 0) return false;
            return true;
        }

        // generate report snapshot (simple LINQ usage)
        public string GenerateReportXml()
        {
            List<JobReportEntry> snapshot;
            lock (_finishedLock)
            {
                snapshot = _finished.ToList();
            }

            var byType = snapshot.GroupBy(x => x.Type).Select(g => new
            {
                Type = g.Key,
                Count = g.Count(),
                Avg = g.Where(x => x.Success).Select(x => x.DurationMs).DefaultIfEmpty(0).Average(),
                Failed = g.Count(x => !x.Success)
            }).ToList();

            var doc = new System.Xml.Linq.XDocument(
                new System.Xml.Linq.XElement("Report",
                    new System.Xml.Linq.XElement("Generated", DateTime.UtcNow),
                    new System.Xml.Linq.XElement("Summary",
                        from t in byType
                        select new System.Xml.Linq.XElement("Type",
                            new System.Xml.Linq.XAttribute("Name", t.Type),
                            new System.Xml.Linq.XElement("Executed", t.Count),
                            new System.Xml.Linq.XElement("AverageDurationMs", (int)t.Avg),
                            new System.Xml.Linq.XElement("Failed", t.Failed)
                        )
                    )
                )
            );

            return doc.ToString();
        }

        public void Dispose()
        {
            try
            {
                _cts?.Cancel();
                try
                {
                    if (_workers.Count > 0)
                        Task.WaitAll(_workers.ToArray());
                }
                catch (OperationCanceledException)
                {
                    // Expected when cancelling
                }
            }
            finally
            {
                _cts?.Dispose();
            }
        }
    }
}
