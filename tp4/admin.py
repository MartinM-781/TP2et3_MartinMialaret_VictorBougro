# %%
from confluent_kafka.admin import AdminClient, NewTopic

# %%
config = {
    'bootstrap.servers': 'localhost:9092',
}

admin_client = AdminClient(config)

# %%
topic = 'moby-dick'

# %%
if topic in admin_client.list_topics(timeout=10).topics:
    admin_client.delete_topics([topic])[topic].result()
    print(f'Deleted topic: {topic}')

# %%
admin_client.create_topics(
    [NewTopic(topic, num_partitions=1, replication_factor=1)]
)[topic].result()
print(f'Created topic: {topic}')

# %%
x = admin_client.list_topics(timeout=10)
for t in sorted(x.topics):
    print(f'{t}  partitions={len(x.topics[t].partitions)}')
