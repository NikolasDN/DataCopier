using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCopier.Models;

public class Settings
{
    public string Source { get; set; }
    public string Destination { get; set; }
    public IList<string> IgnoredTables { get; set; }
    public IList<string> IgnoredDependencies { get; set; }
    public int ChunkSize { get; set; }
}
