using EventStore.Client;
using MythosSoftware.EventSource.Application.Interfaces.Repositories;
using MythosSoftware.EventSource.Data.Serializers;
using MythosSoftware.EventSource.Domain.Entities;
using MythosSoftware.EventSource.Domain.Events;

namespace MythosSoftware.EventSource.Data.Repositories;

public class GenericRepository<T>(EventStoreClient client, EventSerializer serializer) : IGenericEventRepository<T> where T : EntityBase
{
    #region Fields

    private static string _streamName(string id) => $"commission-{typeof(T).Name}-{id:N}";

    #endregion

    #region IGenericRepository

    public async Task<IReadOnlyList<IDomainEvent>> LoadDomainEventsAsync(string id, CancellationToken ct)
    {
        return  await ReadDomainEvents(id, ct);
    }

    public async Task<T> LoadAsync(string id, CancellationToken ct)
    {
        var events = await ReadDomainEvents(id, ct);
        return EntityBase.Rehydrate<T>(id, events);
    }

    public async Task SaveAsync(T entity, CancellationToken ct)
    {
        if (entity.PendingEvents.Count == 0) return;

        var originalVersion = entity.Version - entity.PendingEvents.Count;
        var eventData = entity.PendingEvents.Select(serializer.ToEventData).ToArray();

        if (originalVersion == 0)
        {
            await client.AppendToStreamAsync(_streamName(entity.Id), StreamState.NoStream, eventData, cancellationToken: ct);
        }
        else
        {
            await client.AppendToStreamAsync(_streamName(entity.Id), StreamRevision.FromInt64(originalVersion - 1), eventData, cancellationToken: ct);
        }

        entity.MarkSaved();
    }

    #endregion

    #region Private Methods

    private async Task<IReadOnlyList<IDomainEvent>> ReadDomainEvents(string id, CancellationToken ct)
    {
        var result = client.ReadStreamAsync(Direction.Forwards, _streamName(id),StreamPosition.Start, cancellationToken: ct);
        
        if (await result.ReadState == ReadState.StreamNotFound) return [];

        var domainEvents = new List<IDomainEvent>();
        
        await foreach (var resolved in result)
        {
            domainEvents.Add(serializer.FromResolvedEvent(resolved).Data);
        }

        return domainEvents;
    }

    #endregion
}