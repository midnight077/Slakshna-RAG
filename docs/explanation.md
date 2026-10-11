# SLAKSHNA explained in plain language

This document assumes no knowledge of Rust or Python and only a little of networking.

## 1. The problem the project solves

Several organisations each hold private data, for example hospitals with patient records. They would all benefit from one AI model trained on everyone's data, but nobody is allowed to hand their data to anyone else.

**Federated learning** is the workaround. Each organisation trains the model on its own data, on its own computers. Then they share only *what the model learned* (a list of number adjustments), never the data itself. Everyone blends the adjustments they receive into their own copy of the model, and repeats.

Most federated learning systems have a central server that collects and blends the adjustments. SLAKSHNA has none. Every participant talks directly to the others. That is what **peer-to-peer** means: all participants are equals, with no boss in the middle.

An analogy: a study group where each student studies their own textbook at home. Every ten minutes each student posts a short note saying "here is what I changed in my understanding". Everyone reads the notes, trusts some classmates more than others, and updates their own understanding accordingly. Nobody ever shares their textbook, and there is no teacher.

## 2. The main words

| Word | Meaning here |
| :-- | :-- |
| **Node** | One running copy of the SLAKSHNA program, standing for one participant. Two nodes can sit on two machines or on the same machine. |
| **Peer** | Any other node, seen from one node's point of view. |
| **Federation** | The group of nodes training together. They share a group name, the `federation id`. |
| **Model** | The AI being trained, here a large language model. |
| **LoRA adapter** | A small add-on to the model. Only this add-on is trained, which is far cheaper than retraining the whole model. |
| **Delta** | "What changed": the adapter after training minus the adapter before. This is the note that gets shared. |
| **Epoch / round** | One cycle of train, share, blend. |
| **Trust score** | How much a node believes a given peer's notes are useful. |

## 3. Two halves: a messenger and a brain

Each node is made of two programs written in two languages.

**The messenger (Rust, in the `src/` folder).** It is always running. It finds the other nodes, keeps secure connections to them, sends and receives notes, keeps a record of who sent what, and answers status questions from a web browser or script. It knows nothing about AI. To it, a delta is just a long piece of text to deliver.

**The brain (Python, `ml_engine.py`).** It does the AI work: train, compute the delta, shrink it, blend in the peers' deltas, and score the peers. It knows nothing about networking. It is not always running: the messenger starts it once per round, waits for it to finish, and reads its result.

The two talk in a very simple way. The messenger leaves the peers' notes as files in a folder. The brain reads them, does its work, and prints one final line of text containing its result. The messenger reads that last line. That is why the code is careful that the brain prints nothing else at the end.

Rust was chosen for the messenger because it is fast and reliable for long-running network programs. Python was chosen for the brain because nearly all AI tools are written for it.

## 4. What happens in one round

1. **Wait for the bell.** All nodes wait for the next round boundary on the clock (for example every 10 minutes on the dot). This keeps nodes in step even though nobody coordinates them.
2. **Lay out the mail.** The messenger writes each peer's most recent note into a folder for the brain.
3. **Study.** The brain trains the adapter on the node's own private data for a short while.
4. **Work out what changed.** New adapter minus old adapter gives the delta.
5. **Shrink the note.** The delta is too big to send whole, so two tricks are used:
   - *Sparsification:* keep only the largest 10% of the changes and drop the rest. The dropped part is saved locally and added back next round, so nothing is lost for good.
   - *Quantization:* store each kept number in a rougher, smaller form (like rounding prices to the nearest rupee).
6. **Add privacy protection.** The changes are capped in size and a little random noise can be added, so nobody can work backwards from a note to the private data.
7. **Read the peers' notes.** Each one is checked for being well-formed and not absurdly large, which guards against a peer trying to wreck the model.
8. **Blend.** The brain mixes its own delta with the peers' deltas. Trusted peers count for more.
9. **Update trust.** If a peer's changes point in a similar direction to the node's own, its trust goes up. If they point somewhere odd, it goes down.
10. **Send.** The brain hands the shrunken note and trust scores to the messenger, which sends them to everyone.
11. **Wait** until everyone expected has reported, or a deadline passes, then start again.

After a set number of rounds the node stops by itself.

## 5. How nodes find and talk to each other

### Addresses and ports

Every computer on a network has an **IP address**, like a building's street address. A **port** is like a door number in that building. One computer runs many programs, and each listens at its own door. A SLAKSHNA node uses three doors:

- `p2p_port` (for example 9000): where other nodes knock;
- `api_port` (for example 8545): where a person or script asks "what is your status?";
- `ws_port`: reserved for a live dashboard connection.

Two nodes on one machine must use different door numbers, which is why `node1.toml` and `node2.toml` list different ports.

### Identity

When a node first starts, it creates a **keypair**: a secret key that it keeps and a public key that it shares. The public key acts as the node's permanent name. Anyone can use it to check that a message really came from that node and to lock a message so only that node can open it. The keypair is saved in the node's data folder, so the name survives restarts. Deleting that folder gives the node a new name, and the others no longer recognise it.

### Finding each other without a central directory

Nodes use several methods at once:

- **mDNS:** calling out in the room. On the same local network a node announces "I belong to federation X" and others hear it. No setup needed.
- **DHT:** a public phone book kept jointly by millions of computers on the internet (the same one BitTorrent uses). A node lists "public key → current address" there, and others look it up.
- **Remembered peers:** each node writes down everyone it has ever met and calls them again after a restart.
- **Peer exchange:** once you know one member, they introduce you to the rest.
- **Manual:** you can paste another node's public key, and if needed its address, into the config file.

### Getting through firewalls

Office and campus networks often block incoming connections, like a building where visitors cannot walk in unannounced. Two fallbacks exist:

- a **relay**, a public middleman that both nodes connect *out* to and that passes sealed envelopes between them;
- **Playit.gg**, a service that gives a node a fixed public address.

Neither can read the messages, because of the next point.

### Encryption

Every message between nodes is locked so that only the intended node can open it. The middlemen see sealed envelopes only. The transport used is called **QUIC**, a modern, fast way to send data that has this locking built in. The networking library that provides all of this is called **Iroh**.

### Gossip

To spread a note, a node does not contact everyone. It tells a few neighbours, who tell their neighbours, and so on, the way a rumour spreads. This is called a **gossip protocol**. It keeps working when some nodes are offline and does not need anyone in charge.

## 6. The configuration files

Each node has a `.toml` file. It is a plain settings file. The important parts:

| Section | In plain words |
| :-- | :-- |
| `[federation] id` | The group name. Nodes with the same name train together. A typo here means the node quietly ends up alone. |
| `[training]` | How long a round lasts, how long to wait for others, how many participants to expect. |
| `[compression]` | How hard to shrink the notes. |
| `[node]` | Where this node stores its files, and which graphics cards (GPUs) it may use. |
| `[network]` | The three door numbers, and an optional list of known peers. |
| `[discovery]` | Which of the finding methods above to switch on. |

A separate file, `node_template.yaml`, holds the AI settings: which model, which dataset, how fast to learn.

## 7. Map of the folders

| Path | What it is |
| :-- | :-- |
| `src/main.rs` | The messenger's main routine: the round-by-round loop from section 4. |
| `src/network/` | Finding peers, connecting, gossip. |
| `src/api.rs` | The status web pages (`/status`, `/peers`, `/leaderboard`, `/updates`). |
| `src/history.rs` | The logbook of notes and trust reviews received this session. |
| `src/state.rs` | The small on-disk database: the node's keys, round number, remembered peers. |
| `src/identity.rs` | Creating and using the keypair. |
| `src/config.rs` | Reading the `.toml` settings file. |
| `ml_engine.py` | The brain. |
| `federated_communication/` | The brain's helpers for shrinking, privacy and blending. |
| `Bhaskera/` | A separate project plugged in here, which does the heavy model training across several GPUs. It is an empty folder until you run a download command. |
| `node*.toml`, `examples/` | Ready-made settings for 2, 4 and 6 nodes. |
| `start_cluster.sh`, `stop_cluster.sh` | Start or stop several nodes on one machine with one command. |
| `scripts/`, `plots_script/`, `results/`, `logs/` | Experiment runners, chart makers, and their outputs. |
| `evaluations/` | Tools to turn a trained adapter into a standard format and test it on a benchmark. |
| `docs/` | Documentation, including this file. |

Some files (`Dockerfile`, `deploy.sh`, parts of `scripts/`) are left over from an earlier version of the project and do not match the current code.

## 8. How to see a running node

While a node is running, opening `http://localhost:8545/status` in a browser shows its group name, round number and how many peers it is connected to. `/peers` lists who it is connected to and who it remembers. `/leaderboard` shows trust rankings. These pages are read-only.

## 9. Where the planned RAG feature fits

**RAG** (retrieval-augmented generation) means: before an AI answers a question, look up relevant passages in a document collection and hand them to the AI along with the question, so it answers from real text instead of from memory alone.

The plan (in `docs/p2p_rag_plan.md`) is to make this peer-to-peer:

1. A question arrives at node 1.
2. Node 1 searches its own documents and also forwards the question to node 2 (and later nodes 3, 4, …).
3. Each node searches its own documents and sends back the best passages.
4. Node 1 pools the passages, keeps the best few, and has an AI write the answer.

This reuses the messenger almost entirely: finding peers, identities and encrypted connections are already there. Three things are new:

- a way to *ask one peer a question and get a reply*, since today nodes can only announce things to everyone;
- a second, always-on Python helper per node that does the document search and answer writing;
- a switch to turn off training, because the training part needs powerful GPU machines and the RAG experiments will run on ordinary CPU machines.

## 10. Short glossary

| Term | Meaning |
| :-- | :-- |
| **API** | A set of web addresses a program offers so other programs can ask it things. |
| **CPU / GPU** | The general-purpose processor in every computer / a graphics processor that is much faster for AI training. |
| **Embedding** | A list of numbers representing the meaning of a piece of text, so that similar texts get similar numbers. Used for searching by meaning. |
| **Hash** | A short fingerprint of some data. Change the data and the fingerprint changes. |
| **LAN** | Local network, such as one office or one Wi-Fi. |
| **NAT / firewall** | Network equipment that hides or blocks computers from being contacted from outside. |
| **RocksDB** | The small database the messenger uses to save things to disk. |
| **Submodule** | Another project's code linked inside this one (here, Bhaskera). |
| **TOML / YAML / JSON** | Three text formats for writing settings or data in a structured way. |
