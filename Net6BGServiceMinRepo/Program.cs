using Confluent.Kafka;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();
builder.Services.AddHostedService<BGService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();


public class BGService : BackgroundService
{
    private readonly ILogger<BGService> _logger;


    public BGService(ILogger<BGService> logger)
    {
        _logger = logger;
    }


    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await DoExecute(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.ToString());
        }
    }

    private Task DoExecute(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting events consumer");
        var conf = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "analytics-service",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
        _logger.LogInformation("Building config");
        using (var builder = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            _logger.LogInformation("Subscribing to {0}", "accounts-cud");
            builder.Subscribe("accounts-cud");
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Waiting for new message");
                var messageJson = builder.Consume(stoppingToken);
                _logger.LogInformation("RECEIVED: {0}", messageJson.Message.Value);
            }
            _logger.LogInformation("Done");
        }
        return Task.CompletedTask;
    }
}
