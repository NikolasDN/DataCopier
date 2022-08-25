using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCopier.Models;

public class Table
{
    public Table()
    {
        DependentTables = new List<Table>();
        Columns = new List<Column>();
        ColumnsToIgnore = new List<string>();
    }
    public string Schema { get; set; }
    public string Name { get; set; }

    public string Fullname()
    {
        return Schema + "." + Name;
    }

    public IList<Table> DependentTables { get; set; }

    public IList<Column> Columns { get; set; }
    public IList<string> ColumnsToIgnore { get; set; }
}
