use iroh_bitswap::{Bitswap, BitswapEvent, Config as BitswapConfig, Store};
use libp2p::relay::v2::{
    client::Client as RelayClient, client::Event as RelayClientEvent, relay::Event as RelayEvent,
    relay::Relay,
};
use libp2p::PeerId;
use libp2p::{
    dcutr, relay,
    swarm::{behaviour::toggle::Toggle, NetworkBehaviour},
};

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event", event_process = false)]
pub struct PopTartBehaviour<S: Store> {
    relay_client: Toggle<RelayClient>,
    relay: Toggle<Relay>,
    dcutr: Toggle<dcutr::behaviour::Behaviour>,
    pub bitswap: Bitswap<S>,
}

unsafe impl<S: Store> Send for PopTartBehaviour<S> {}
unsafe impl<S: Store> Sync for PopTartBehaviour<S> {}

impl<S: Store> PopTartBehaviour<S> {
    pub async fn new(
        peer_id: &PeerId,
        store: S,
        is_relay_client: bool,
        relay_client: Option<relay::v2::client::Client>,
    ) -> Self {
        let (dcutr, relay_client) = if is_relay_client {
            let relay_client =
                relay_client.expect("missing relay client even though it was enabled");
            let dcutr = dcutr::behaviour::Behaviour::new();
            (Some(dcutr), Some(relay_client))
        } else {
            (None, None)
        };

        let relay = if !is_relay_client {
            let config = relay::v2::relay::Config::default();
            let r = Relay::new(*peer_id, config);
            Some(r)
        } else {
            None
        }
        .into();

        PopTartBehaviour {
            dcutr: dcutr.into(),
            relay_client: relay_client.into(),
            relay,
            bitswap: Bitswap::new(*peer_id, store.clone(), BitswapConfig::default()).await,
        }
    }
}

#[derive(Debug)]
pub enum Event {
    Bitswap(BitswapEvent),
    RelayClient(RelayClientEvent),
    Relay(RelayEvent),
    Dcutr(dcutr::behaviour::Event),
}

impl From<BitswapEvent> for Event {
    fn from(e: BitswapEvent) -> Self {
        Event::Bitswap(e)
    }
}

impl From<RelayClientEvent> for Event {
    fn from(e: RelayClientEvent) -> Self {
        Event::RelayClient(e)
    }
}
impl From<relay::v2::relay::Event> for Event {
    fn from(event: relay::v2::relay::Event) -> Self {
        Event::Relay(event)
    }
}

impl From<dcutr::behaviour::Event> for Event {
    fn from(event: dcutr::behaviour::Event) -> Self {
        Event::Dcutr(event)
    }
}
