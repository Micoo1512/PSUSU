using System;
using System.Collections.Generic;
using System.Xml.Linq;
using Kolokvijum1.Models;

namespace Kolokvijum1.Processing
{
    public class SystemJobConfig
    {
        public JobType Type { get; set; }
        public string Payload { get; set; } = string.Empty;
        public int Priority { get; set; }
    }

    public class SystemConfig
    {
        public int WorkerCount { get; set; }
        public int MaxQueueSize { get; set; }
        public List<SystemJobConfig> Jobs { get; } = new List<SystemJobConfig>();

        public static SystemConfig Load(string path)
        {
            var doc = XDocument.Load(path);
            var root = doc.Root;
            var cfg = new SystemConfig();
            if (root == null) return cfg;
            cfg.WorkerCount = int.Parse(root.Element("WorkerCount")?.Value ?? "1");
            cfg.MaxQueueSize = int.Parse(root.Element("MaxQueueSize")?.Value ?? "100");
            var jobs = root.Element("Jobs");
            if (jobs != null)
            {
                foreach (var j in jobs.Elements("Job"))
                {
                    var typeStr = j.Attribute("Type")?.Value ?? "IO";
                    if (!Enum.TryParse<JobType>(typeStr, out var jt)) jt = JobType.IO;
                    var payload = j.Attribute("Payload")?.Value ?? string.Empty;
                    var pr = int.Parse(j.Attribute("Priority")?.Value ?? "0");
                    cfg.Jobs.Add(new SystemJobConfig { Type = jt, Payload = payload, Priority = pr });
                }
            }
            return cfg;
        }
    }
}
