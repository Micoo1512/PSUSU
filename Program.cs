using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Kolokvijum1.Models;
using Kolokvijum1.Processing;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("Starting ProcessingSystem from config...");
        var configPath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "SystemConfig.xml");
        if (!File.Exists(configPath)) configPath = Path.Combine(AppContext.BaseDirectory, "SystemConfig.xml");

        if (!File.Exists(configPath))
        {
            Console.WriteLine($"Config file not found: {configPath}");
            return;
        }

        var cfg = Kolokvijum1.Processing.SystemConfig.Load(configPath);

        using var ps = new ProcessingSystem(cfg.WorkerCount, cfg.MaxQueueSize);

        // subscribe to events - use simple lambda expressions to asynchronously log
        var logDir = Path.Combine(AppContext.BaseDirectory, "logs");
        Directory.CreateDirectory(logDir);
        var logFile = Path.Combine(logDir, "events.log");
        var logLock = new object();

        ps.JobCompleted += (job, result) =>
        {
            Task.Run(() =>
            {
                var line = $"[{DateTime.Now:O}] [COMPLETED] {job.Id}, Result={result}";
                lock (logLock) File.AppendAllText(logFile, line + Environment.NewLine);
            });
        };

        ps.JobFailed += (job, status) =>
        {
            Task.Run(() =>
            {
                var line = $"[{DateTime.Now:O}] [FAILED:{status}] {job.Id}, Result=NA";
                lock (logLock) File.AppendAllText(logFile, line + Environment.NewLine);
            });
        };

        // start generator threads: each thread randomly submits jobs
        var cts = new CancellationTokenSource();
        var gens = new Task[cfg.WorkerCount];
        var rnd = System.Random.Shared;

        for (int i = 0; i < cfg.WorkerCount; i++)
        {
            gens[i] = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        // randomly create Prime or IO
                        var type = (rnd.Next(0, 2) == 0) ? JobType.Prime : JobType.IO;
                        string payload;
                        if (type == JobType.Prime)
                        {
                            var numbers = (rnd.Next(5) + 1) * 2000; // 2000..10000
                            var threads = rnd.Next(1, 9);
                            payload = $"numbers:{numbers},threads:{threads}";
                        }
                        else
                        {
                            var delay = (rnd.Next(5) + 1) * 500; // 500..3000 ms
                            payload = $"delay:{delay}";
                        }

                        var job = new Job { Id = Guid.NewGuid(), Type = type, Payload = payload, Priority = rnd.Next(1, 4) };
                        try
                        {
                            ps.Submit(job);
                            Console.WriteLine($"Generator submitted {job.Id} Type={job.Type} Payload={job.Payload} Priority={job.Priority}");
                        }
                        catch (Exception ex)
                        {
                            // queue full - ignore or log
                            Console.WriteLine($"Submit failed: {ex.Message}");
                        }

                        // wait a bit
                        await Task.Delay(rnd.Next(200, 800), cts.Token).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException) { break; }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Generator error: {ex.Message}");
                    }
                }
            });
        }

        // report timer: every minute generate report XML and keep last 10 in files
        var reportDir = Path.Combine(AppContext.BaseDirectory, "reports");
        Directory.CreateDirectory(reportDir);
        int reportCounter = 0;
        var reportTask = Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(1), cts.Token).ConfigureAwait(false);
                }
                catch (TaskCanceledException) { break; }

                try
                {
                    var xml = ps.GenerateReportXml();
                    var file = Path.Combine(reportDir, $"report_{reportCounter % 10}.xml");
                    File.WriteAllText(file, xml);
                    reportCounter++;
                    Console.WriteLine($"Report written: {file}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Report error: {ex.Message}");
                }
            }
        });

        Console.WriteLine("System running. Press Enter to stop...");
        Console.ReadLine();

        // shutdown
        cts.Cancel();
        
        try 
        { 
            await Task.WhenAll(gens).ConfigureAwait(false); 
        } 
        catch (OperationCanceledException) 
        { 
            // Expected when cancelling
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Generator shutdown error: {ex.Message}");
        }

        try 
        { 
            await reportTask.ConfigureAwait(false); 
        } 
        catch (OperationCanceledException)
        {
            // Expected when cancelling
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Report task shutdown error: {ex.Message}");
        }

        ps.Dispose();
        cts.Dispose();

        Console.WriteLine("Stopped.");
    }
}
