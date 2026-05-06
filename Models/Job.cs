using System;

namespace Kolokvijum1.Models
{
    public enum JobType { Prime, IO }

    public class Job
    {
        public Guid Id { get; set; }
        public JobType Type { get; set; }
        public string Payload { get; set; } = string.Empty;
        public int Priority { get; set; }
    }
}
