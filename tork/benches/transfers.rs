use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::prelude::*;
use libipld::{ipld, Ipld};
use libp2p::{
    swarm::{Swarm, SwarmEvent},
    Multiaddr,
};
use rand::prelude::*;
use tokio::{runtime::Runtime, sync::mpsc};
use tork::{create_block, encode_ipld, mk_transport, Store, TestStore, Tork};

pub fn transfers_benchmark(c: &mut Criterion) {
    static MB: usize = 1024 * 1024;

    let mut group = c.benchmark_group("transfer_random_bytes");

    for size in [MB].iter() {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("file_size", size),
            size,
            move |b, &size| {
                const NP: usize = 4;
                let executor = Runtime::new().unwrap();

                let mut data = vec![0u8; size];
                rand::thread_rng().fill_bytes(&mut data);

                const CHUNK_SIZE: usize = 250 * 1024;

                let store = TestStore::default();
                let chunks = data.chunks(CHUNK_SIZE);

                let links: Vec<Ipld> = chunks
                    .map(|chunk| {
                        // encoding as raw
                        let (cid, _, _) = create_block(chunk.to_vec(), 0x55);
                        store.put(cid, chunk, vec![]).ok();
                        let link = ipld!({
                            "Hash": cid,
                            "Tsize": CHUNK_SIZE,
                        });
                        link
                    })
                    .collect();

                let root_node = ipld!({
                    "Links": links,
                });

                let buf = encode_ipld(0x71, &root_node);
                let (root, bytes, _) = create_block(buf, 0x71);

                store.put(root, bytes, vec![]).unwrap();

                let (client, root) = executor.block_on(async {
                    let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);

                    for _ in 0..NP {
                        let (peer_id, trans) = mk_transport();
                        let t = Tork::new(peer_id, store.clone());
                        let mut provider = Swarm::with_tokio_executor(trans, t, peer_id);

                        Swarm::listen_on(&mut provider, "/ip4/127.0.0.1/tcp/0".parse().unwrap())
                            .unwrap();

                        let sender = tx.clone();
                        tokio::task::spawn(async move {
                            while provider.next().now_or_never().is_some() {}
                            let listeners: Vec<_> = Swarm::listeners(&provider).collect();
                            for l in listeners {
                                sender.send(l.clone()).await.unwrap();
                            }

                            loop {
                                let _ev = provider.next().await;
                            }
                        });
                    }

                    let (peer_id, trans) = mk_transport();
                    let t = Tork::new(peer_id, TestStore::default());
                    let mut client_swarm = Swarm::with_tokio_executor(trans, t, peer_id);

                    let client = client_swarm.behaviour().clone();

                    for _ in 0..NP {
                        let addr = rx.recv().await.unwrap();
                        Swarm::dial(&mut client_swarm, addr).unwrap();
                    }

                    let mut conns = 0;

                    loop {
                        match client_swarm.next().await {
                            Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                                conns += 1;
                                if conns == NP {
                                    break;
                                }
                            }
                            _ => (),
                        }
                    }

                    tokio::task::spawn(async move {
                        loop {
                            let _ev = client_swarm.next().await;
                        }
                    });

                    (client, root)
                });

                b.to_async(&executor).iter(|| {
                    let client = client.clone();
                    async move {
                        let session = client.new_session().unwrap();
                        let content = session.resolve_all(root).await;

                        let res = content.try_collect::<Vec<Ipld>>().await.unwrap();
                        black_box(res)
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, transfers_benchmark);
criterion_main!(benches);
