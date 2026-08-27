using DataIngestor.Channels;
using DataIngestor.Ingestion;
using DataIngestor.Processing;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
builder.Services.AddHostedService<KafkaConsumer>();
builder.Services.AddSingleton<TelemetryFilter>();
builder.Services.AddSingleton<ChannelRegistry>();
builder.Services.AddSingleton<RtspListener>();
builder.Services.AddSingleton<TelemetryProcessor>();


// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseAuthorization();

app.MapControllers();

app.Run();
