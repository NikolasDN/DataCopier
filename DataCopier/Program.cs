// See https://aka.ms/new-console-template for more information
using DataCopier.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

Console.WriteLine("Hello, DataCopier!");

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        services
            .AddSingleton<ITableCopier, TableCopier>()
            .AddSingleton<IDatabaseCopier, DatabaseCopier>();
    }
);

var host = builder.Build();
using IServiceScope serviceScope = host.Services.CreateScope();
IServiceProvider provider = serviceScope.ServiceProvider;
var databaseCopier = provider.GetRequiredService<IDatabaseCopier>();
databaseCopier.Copy();

await host.RunAsync();

Console.WriteLine("Goodbye, DataCopier!");