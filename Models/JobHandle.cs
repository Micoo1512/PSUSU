using System;
using System.Threading.Tasks;

namespace Kolokvijum1.Models
{
    public class JobHandle
    {
        public Guid Id { get; set; }
        public Task<int> Result { get; set; } = Task.FromResult(0);
    }
}
