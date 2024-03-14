using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
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

    public string GetPrimaryKeyColumn(string connectionString)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            string query = @"SELECT ColumnName = col.column_name 
    FROM information_schema.table_constraints tc
    INNER JOIN information_schema.key_column_usage col
        ON col.Constraint_Name = tc.Constraint_Name
    AND col.Constraint_schema = tc.Constraint_schema
    WHERE tc.Constraint_Type = 'Primary Key' AND col.Table_name = '" + Name + "'";
            //var query = "SELECT COLUMN_NAME\r\nFROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE\r\nWHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1\r\nAND TABLE_NAME = '" + Name + "' AND TABLE_SCHEMA = '" + Schema + "'";

            //define the SqlCommand object
            SqlCommand cmd = new SqlCommand(query, conn);

            //open connection
            conn.Open();

            //execute the SQLCommand
            var result = (string)cmd.ExecuteScalar();

            //close connection
            conn.Close();

            return result;
        }
    }
}
