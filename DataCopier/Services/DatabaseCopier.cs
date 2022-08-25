using DataCopier.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataCopier.Services;

public interface IDatabaseCopier
{
    void Copy();
}
public class DatabaseCopier : IDatabaseCopier
{
    private readonly Settings _settings;
    private readonly ITableCopier _tableCopier;
    public DatabaseCopier(IConfiguration configuration, ITableCopier tableCopier)
    {
        _settings = configuration.GetSection("Settings").Get<Settings>();
        _tableCopier = tableCopier;
    }
    private bool IgnoreTable(Table table)
    {
        foreach (var ignoreTables in _settings.IgnoredTables)
        {
            if (table.Fullname().StartsWith(ignoreTables))
            {
                return true;
            }
        }
        return false;
    }
    public void Copy()
    {
        Console.WriteLine($"source: {_settings.Source}");
        var tables = GetAllTables(_settings.Source);

        // Detect dependencies
        var notConvertedTables = new List<Table>();
        foreach (var table in tables)
        {
            bool copyTable = !IgnoreTable(table);            

            if (copyTable)
            {
                Console.WriteLine($" checking table dependencies: {table.Fullname()}");

                table.DependentTables = _tableCopier.GetForeignKeyTables(_settings.Source, table).Where(f => !IgnoreTable(f)).ToList();
                table.Columns = _tableCopier.GetColumns(_settings.Source, table);
                if (table.DependentTables.Any())
                {
                    var dependentTables = table.DependentTables.Select(s => $"{table.Fullname()}.{s.Name}");
                    var dependentTablesToIgnore = _settings.IgnoredDependencies.Intersect(dependentTables);
                    foreach (var dependentTableString in dependentTablesToIgnore)
                    {
                        var dependentTable = dependentTableString.Split('.').Last();
                        //var reverseDependentTable = dependentTableString.Split('.')[1];
                        var dependentTableToRemove = table.DependentTables.Single(s => s.Name == dependentTable);
                        table.DependentTables.Remove(dependentTableToRemove);
                        table.ColumnsToIgnore.Add(dependentTable + "Id");
                    }
                }                
                Console.WriteLine($" fks: {string.Join(", ", table.DependentTables.Select(s => s.Fullname()))}");
                notConvertedTables.Add(table);
            }            
        }

        // Determine order of copying
        var amountOfTablesToConvert = notConvertedTables.Count;
        var tablesToCopy = new List<Table>();
        while (amountOfTablesToConvert > 0 )
        {
            var tablesToConvert = new List<Table>();
            foreach (var table in notConvertedTables)
            {
                var dependencies = notConvertedTables.SelectMany(s => s.DependentTables.Select(ss => ss.Fullname()));
                if (dependencies.Contains(table.Fullname()))
                {
                    // cannot copy yet
                }
                else
                {
                    Console.WriteLine($" dry-running copy table: {table.Fullname()}");
                    tablesToConvert.Add(table);
                }
            }
            foreach(var tableToConvert in tablesToConvert)
            {
                notConvertedTables.Remove(tableToConvert);
                tablesToCopy.Add(tableToConvert);
            }

            if (amountOfTablesToConvert == notConvertedTables.Count)
            {
                Console.WriteLine("Recursive references prevent copying, please add IgnoredDependencies");
                Console.WriteLine($" not converted tables: {string.Join(", ", notConvertedTables.Select(s => s.Fullname()))}");
                var dependencies = notConvertedTables.SelectMany(s => s.DependentTables.Select(ss => ss.Fullname()));
                Console.WriteLine($" remaining dependencies: {string.Join(", ", dependencies)}");
                break;
            }
            amountOfTablesToConvert = notConvertedTables.Count;
        }

        // start copying
        foreach (var tableToCopy in tablesToCopy)
        {
            _tableCopier.Copy(_settings.Source, _settings.Destination, tableToCopy);
        }

        // fix missing FK links for circularly dependent tables
        foreach (var tableToCopy in tablesToCopy.Where(f => f.ColumnsToIgnore.Any()))
        {
            _tableCopier.UpdateLinks(_settings.Source, _settings.Destination, tableToCopy);
        }
    }

    private IList<Table> GetAllTables(string connectionString)
    {
        var result = new List<Table>();
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            string query = @"SELECT 
    TABLE_SCHEMA, TABLE_NAME
FROM
    information_schema.tables;
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
                    result.Add(new Table() { Schema = dr.GetString(0), Name = dr.GetString(1) });
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


}
