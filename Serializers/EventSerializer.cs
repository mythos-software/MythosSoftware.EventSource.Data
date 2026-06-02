using System.Text.Json;
using EventStore.Client;
using MythosSoftware.EventSource.Domain.Events;

namespace MythosSoftware.EventSource.Data.Serializers;

public class EventSerializer
{
    private readonly JsonSerializerOptions _options = new(JsonSerializerDefaults.Web);

    private readonly Dictionary<string, Type> _eventTypes = typeof(IDomainEvent).Assembly
        .GetTypes()
        .Where(t => t is { IsPublic: true, IsSealed: true, IsClass: true } && typeof(IDomainEvent).IsAssignableFrom(t))
        .ToDictionary(t => t.Name, t => t);

    public EventData ToEventData(EventEnvelope envelope) => new(
        Uuid.NewUuid(),
        envelope.Data.GetType().Name,
        JsonSerializer.SerializeToUtf8Bytes(envelope.Data, envelope.Data.GetType(), _options),
        JsonSerializer.SerializeToUtf8Bytes(envelope.Metadata, _options));

    public EventEnvelope FromResolvedEvent(ResolvedEvent resolved)
    {
        var typeName = resolved.Event.EventType;
        
        if (!_eventTypes.TryGetValue(typeName, out var clrType))
        {
            throw new NotSupportedException($"Unknown event type in EventStoreDB: {typeName}");
        }

        var data = (IDomainEvent)JsonSerializer.Deserialize(resolved.Event.Data.Span, clrType, _options)!;
        var metadata = resolved.Event.Metadata.Length == 0
            ? new EventMetadata("unknown", "unknown", "unknown", DateTimeOffset.MinValue)
            : JsonSerializer.Deserialize<EventMetadata>(resolved.Event.Metadata.Span, _options)!;

        return new EventEnvelope(data, metadata);
    }
}