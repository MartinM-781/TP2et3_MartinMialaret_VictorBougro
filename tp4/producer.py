# %%
import socket
import time
import urllib.request
from pathlib import Path
from confluent_kafka import Producer

# %%
book_url = 'https://www.gutenberg.org/ebooks/2701.txt.utf-8'
book_path = Path('data/moby_dick.txt')

if not book_path.exists():
    book_path.parent.mkdir(exist_ok=True)
    urllib.request.urlretrieve(book_url, book_path)
print(f'Book: {book_path} ({book_path.stat().st_size:,} bytes)')

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

producer = Producer(conf)

# %%
topic = 'moby-dick'
DELAY = 0

# %%
delivered = 0
failed = 0


def on_delivery(err, msg):
    global delivered, failed
    if err is None:
        delivered += 1
    else:
        failed += 1
        print(f'Line {msg.key().decode()} not delivered: {err}')


# %% Streaming Query
start_time = time.perf_counter()
sent = 0

with open(book_path, encoding='utf-8-sig') as book:
    for line_number, line in enumerate(book, start=1):
        producer.produce(
            topic=topic,
            key=str(line_number),
            value=line.rstrip('\n'),
            on_delivery=on_delivery
        )
        producer.poll(0)
        sent += 1
        if sent % 5000 == 0:
            print(f'{sent:,} lines sent')
        if DELAY:
            time.sleep(DELAY)

producer.flush()
elapsed = time.perf_counter() - start_time
print(f'{sent:,} lines sent, {delivered:,} delivered, {failed} failed '
      f'in {elapsed:.1f} s')
producer.close()
