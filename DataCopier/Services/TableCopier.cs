using DataCopier.Models;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;

namespace DataCopier.Services;

public interface ITableCopier
{
    void Copy(string source, string destination, Table table);
    void UpdateLinks(string source, string destination, Table table);
    IList<Column> GetColumns(string connectionString, Table table);
    IList<Table> GetForeignKeyTables(string connectionString, Table table);
}
public class TableCopier : ITableCopier
{
    private readonly Settings _settings;
    public TableCopier(IConfiguration configuration)
    {
        _settings = configuration.GetSection("Settings").Get<Settings>();
    }

    public void Copy(string source, string destination, Table table)
    {
        var count = GetCount(source, table);
        var counter = 0;
        var offset = 0;
        var rows = GetList(source, table, offset, _settings.ChunkSize);
        var rowsCopied = rows.Count;
        using (var progressBar = new ProgressBar())
        {            
            InsertList(destination, table, rows, progressBar, count, ref counter);            
            while (rows.Count == _settings.ChunkSize)
            {
                offset += _settings.ChunkSize;
                rows = GetList(source, table, offset, _settings.ChunkSize);
                InsertList(destination, table, rows, progressBar, count, ref counter);
                rowsCopied += rows.Count;
            }
        }
        Console.WriteLine($" {table.Fullname()}, {rowsCopied} rows copied");
    }

    private void InsertList(string connectionString, Table table, List<object[]> rows, ProgressBar progressBar, int count, ref int counter)
    {
        //sql connection object
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            //open connection
            conn.Open();

            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                SqlCommand cmd1 = new SqlCommand($"SET IDENTITY_INSERT {table.Fullname()} ON", conn, transaction);
                cmd1.ExecuteNonQuery();
                transaction.Commit();
            }

            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                foreach (var row in rows)
                {
                    // formatting
                    for (int i = 0; i < table.Columns.Count; i++)
                    {
                        if (table.Columns[i].Type == "datetime2" || table.Columns[i].Type == "date")
                        {
                            if (DateTime.TryParse(row[i].ToString(), out DateTime dateTime))
                            {
                                row[i] = dateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
                            }
                        }
                        if (row[i].ToString().Contains("'"))
                        {
                            row[i] = row[i].ToString().Replace("'", "''");
                        }
                        if (table.Columns[i].Type == "decimal")
                        {
                            row[i] = row[i].ToString().Replace(",", ".");
                        }
                        if (table.Columns[i].Type == "varbinary")
                        {
                            var content = (byte[])row[i];
                            var str = BitConverter.ToString(content, 0, content.Length);
                            str = str.Replace("-", "");
                            row[i] = $"0x{str}";
                        }
                        else
                        {
                            if (row[i].ToString() == string.Empty && table.Columns[i].IsNullable == "YES")
                            {
                                row[i] = "NULL";
                            }
                            else
                            {
                                row[i] = $"'{row[i]}'";
                            }
                            if (table.ColumnsToIgnore.Contains(table.Columns[i].Name) && table.Columns[i].IsNullable == "YES")
                            {
                                row[i] = "NULL";
                            }
                        }
                    }

                    //execute the SQLCommand
                    string query = @$"
IF NOT EXISTS ( SELECT 1 FROM {table.Fullname()} WHERE Id = {row[0]})
BEGIN
    INSERT INTO {table.Fullname()}
                ({string.Join(", ", table.Columns.Select(s => s.Name))})
                VALUES ({string.Join(", ", row)}
)
END";

                    //define the SqlCommand object
                    SqlCommand cmd = new SqlCommand(query, conn, transaction);
                    cmd.ExecuteNonQuery();

                    counter++;
                    progressBar.Report(((double)counter / ((double)count)));
                }
                transaction.Commit();
            }

            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                SqlCommand cmd2 = new SqlCommand($"SET IDENTITY_INSERT {table.Fullname()} OFF", conn, transaction);
                cmd2.ExecuteNonQuery();
                transaction.Commit();
            }

            //close connection
            conn.Close();
        }
    }

    private List<object[]> GetList(string connectionString, Table table, int offset, int size)
    {
        var result = new List<object[]>();
        //sql connection object
        using (SqlConnection conn = new SqlConnection(connectionString))
        {

            //retrieve the SQL Server instance version
            string query = @$"SELECT *
                                     FROM {table.Fullname()}
WHERE Id > 0
ORDER BY Id
OFFSET     {offset} ROWS       -- skip rows
FETCH NEXT {size} ROWS ONLY; -- take rows
                                     ";
            //define the SqlCommand object
            SqlCommand cmd = new SqlCommand(query, conn);

            //open connection
            conn.Open();

            //execute the SQLCommand
            SqlDataReader dr = cmd.ExecuteReader();

            //check if there are records
            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    object[] row = new object[dr.FieldCount];
                    dr.GetValues(row);
                    result.Add(row);
                }
            }
            
            //close data reader
            dr.Close();

            //close connection
            conn.Close();
        }
        return result;
    }

    private int GetCount(string connectionString, Table table)
    {
        int result = 0;
        //sql connection object
        using (SqlConnection conn = new SqlConnection(connectionString))
        {

            //retrieve the SQL Server instance version
            string query = @$"SELECT count(1)
                                     FROM {table.Fullname()}
";
            //define the SqlCommand object
            SqlCommand cmd = new SqlCommand(query, conn);

            //open connection
            conn.Open();

            //execute the SQLCommand
            result = Convert.ToInt32(cmd.ExecuteScalar());

            //close connection
            conn.Close();
        }
        return result;
    }

    public IList<Table> GetForeignKeyTables(string connectionString, Table table)
    {
        var result = new List<Table>();
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            string query = @$"EXEC sp_fkeys @pktable_name = '{table.Name}', @pktable_owner = '{table.Schema}'
                                     ";
            //define the SqlCommand object
            SqlCommand cmd = new SqlCommand(query, conn);

            //open connection
            conn.Open();

            //execute the SQLCommand
            SqlDataReader dr = cmd.ExecuteReader();

            //check if there are records
            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    result.Add(new Table() { Schema = dr.GetString(5), Name = dr.GetString(6) });
                    //display retrieved record
                    //Console.WriteLine("{0},{1},{2},{3},{4},{5}", empID.ToString(), empCode, empFirstName, empLastName, locationCode, locationDescr);
                }
            }
            
            //close data reader
            dr.Close();

            //close connection
            conn.Close();
        }

        return result;
    }

    public IList<Column> GetColumns(string connectionString, Table table)
    {
        var result = new List<Column>();
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            string query = @$"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = N'{table.Name}'
                                     ";
            //define the SqlCommand object
            SqlCommand cmd = new SqlCommand(query, conn);

            //open connection
            conn.Open();

            //execute the SQLCommand
            SqlDataReader dr = cmd.ExecuteReader();

            //check if there are records
            if (dr.HasRows)
            {
                while (dr.Read())
                {
                    result.Add(new Column() { Name = dr.GetString(0), Type = dr.GetString(1), IsNullable = dr.GetString(2) });
                    //display retrieved record
                    //Console.WriteLine("{0},{1},{2},{3},{4},{5}", empID.ToString(), empCode, empFirstName, empLastName, locationCode, locationDescr);
                }
            }

            //close data reader
            dr.Close();

            //close connection
            conn.Close();
        }

        return result;
    }

    public void UpdateLinks(string source, string destination, Table table)
    {
        var count = GetCount(source, table);
        var counter = 0;
        var offset = 0;
        var rows = GetList(source, table, offset, _settings.ChunkSize);
        var rowsCopied = rows.Count;
        using (var progressBar = new ProgressBar())
        {
            UpdateLinks(destination, table, rows, progressBar, count, ref counter);
            while (rows.Count == _settings.ChunkSize)
            {
                offset += _settings.ChunkSize;
                rows = GetList(source, table, offset, _settings.ChunkSize);
                UpdateLinks(destination, table, rows, progressBar, count, ref counter);
                rowsCopied += rows.Count;
            }
        }
        Console.WriteLine($" {table.Fullname()}, {rowsCopied} links fixed");
    }

    private void UpdateLinks(string connectionString, Table table, List<object[]> rows, ProgressBar progressBar, int count, ref int counter)
    {
        var fkField = table.ColumnsToIgnore.First();

        //sql connection object
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            //open connection
            conn.Open();

            using (SqlTransaction transaction = conn.BeginTransaction())
            {
                foreach (var row in rows)
                {
                    string fkValue = null;
                    for (int i = 0; i < row.Length; i++)
                    {
                        if (table.Columns[i].Name == fkField)
                        {
                            fkValue = row[i].ToString();
                        }
                    }

                    if (!string.IsNullOrEmpty(fkValue))
                    {
                        //execute the SQLCommand
                        string query = @$"UPDATE {table.Fullname()} SET {fkField} = {fkValue} WHERE Id = {row[0]};";

                        //define the SqlCommand object
                        SqlCommand cmd = new SqlCommand(query, conn, transaction);
                        cmd.ExecuteNonQuery();
                    }

                    counter++;
                    progressBar.Report(((double)counter / ((double)count)));
                }
                transaction.Commit();
            }

            //close connection
            conn.Close();
        }
    }
}
