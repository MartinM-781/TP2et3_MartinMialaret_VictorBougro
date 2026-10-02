# %%
import csv
import string
from collections import Counter
from pathlib import Path
from confluent_kafka import Consumer, OFFSET_BEGINNING

# %%
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'moby-dick-cleaner',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False}

consumer = Consumer(conf)

# %%
topic = 'moby-dick'


def replay_from_start(consumer, partitions):
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


consumer.subscribe([topic], on_assign=replay_from_start)

# %%
STOP_WORDS = frozenset({
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "aren't", "as", "at", "be", "because", "been",
    "before", "being", "below", "between", "both", "but", "by", "can",
    "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
    "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
    "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
    "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
    "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its",
    "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no",
    "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
    "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
    "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
    "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
    "then", "there", "there's", "these", "they", "they'd", "they'll",
    "they're", "they've", "this", "those", "through", "to", "too", "under",
    "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're",
    "we've", "were", "weren't", "what", "what's", "when", "when's", "where",
    "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're",
    "you've", "your", "yours", "yourself", "yourselves",
    "s", "t", "said", "upon", "shall", "must", "may", "one", "two", "will",
    "ye", "thou", "thee", "thy", "thine",
})

PUNCTUATION = string.punctuation + '“”‘’«»—–…‹›„'
NORMALIZE = str.maketrans({
    '’': "'", '‘': "'",
    '—': ' ', '–': ' ', '…': ' ',
    '\u00a0': ' ',
    '\u200e': None, '\u200f': None,
})


def clean(line):
    words = []
    for word in line.lower().translate(NORMALIZE).split():
        word = word.strip(PUNCTUATION)
        if word in STOP_WORDS:
            continue
        word = word.removesuffix("'s")
        if (len(word) > 1 and word not in STOP_WORDS
                and not any(char.isdigit() for char in word)):
            words.append(word)
    return words


# %%
output_dir = Path('output')
output_dir.mkdir(exist_ok=True)
clean_path = output_dir / 'moby_dick_clean.txt'
counts_path = output_dir / 'moby_dick_word_counts.csv'

# %%
# Configuration
MAX_JOIN_POLLS = 30
MAX_EMPTY_POLLS = 10  # Ends after ~10 seconds of silence
MAX_ERRORS = 5        # Ends after 5 consecutive errors
join_polls = 0
empty_polls = 0
error_count = 0

section = 'header'
received = 0
licence_lines = 0
book_lines = 0
lines_written = 0
previous_line = 0
out_of_sequence = 0
word_counts = Counter()

with open(clean_path, 'w', encoding='utf-8', newline='\n') as out:
    try:
        while True:
            msg = consumer.poll(1.0)

            # 1. Handle "No Message" (Timeout)
            if msg is None:
                if not consumer.assignment():
                    join_polls += 1
                    if join_polls >= MAX_JOIN_POLLS:
                        print("Closing: No partition assigned, "
                              "is Kafka running?")
                        break
                    continue
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    print("Closing: No new messages received.")
                    break
                continue

            # 2. Handle Errors
            if msg.error():
                error_count += 1
                print(f"Consumer error: {msg.error()}")
                if error_count >= MAX_ERRORS:
                    print("Closing: Too many consecutive errors.")
                    break
                continue

            # 3. Handle Success
            # Reset counters when we actually get data
            empty_polls = 0
            error_count = 0
            received += 1

            line_number = int(msg.key().decode('utf-8'))
            if line_number != previous_line + 1:
                out_of_sequence += 1
            previous_line = line_number

            line = (msg.value() or b'').decode('utf-8')
            if line.startswith('*** START OF'):
                section = 'book'
                licence_lines += 1
            elif line.startswith('*** END OF'):
                section = 'footer'
                licence_lines += 1
            elif section != 'book':
                licence_lines += 1
            else:
                book_lines += 1
                words = clean(line)
                if words:
                    out.write(' '.join(words) + '\n')
                    word_counts.update(words)
                    lines_written += 1

            if received % 5000 == 0:
                print(f'{received:,} messages processed')
    except KeyboardInterrupt:
        print("Closing: Interrupted.")

# Clean up
consumer.close()

# %%
ranking = sorted(word_counts.items(), key=lambda kv: (-kv[1], kv[0]))
with open(counts_path, 'w', encoding='utf-8', newline='') as f:
    writer = csv.writer(f, lineterminator='\n')
    writer.writerow(['word', 'count'])
    writer.writerows(ranking)

print(f'{received:,} messages received, {out_of_sequence} out of sequence')
print(f'{book_lines:,} lines of book, {licence_lines} lines of licence '
      f'skipped')
print(f'{lines_written:,} cleaned lines written to {clean_path}')
print(f'{sum(word_counts.values()):,} words, {len(word_counts):,} distinct, '
      f'counted in {counts_path}')
print('Most frequent words:')
for word, count in ranking[:20]:
    print(f'{count:7,}  {word}')
