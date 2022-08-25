using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCopier.Models;

public class Column
{
    public string Name { get; set; }
    public string Type { get; set; }
    public string IsNullable { get; set; }
}
