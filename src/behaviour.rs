use iroh_bitswap::{Bitswap, BitswapEvent, Config as BitswapConfig, Store};
use libp2p::relay::v2::{
    client::Client as RelayClient, client::Event as RelayClientEvent, relay::Event as RelayEvent,
    relay::Relay,
};
use libp2p::{
    dcutr::behaviour::Behaviour as Dcutr,
    dcutr::behaviour::Event as DcutrEvent,
    identify::Behaviour as Identify,
    identify::Config as IdentifyConfig,
    identify::Event as IdentifyEvent,
    identity::Keypair,
    relay,
    swarm::{behaviour::toggle::Toggle, NetworkBehaviour},
};

pub const PROTOCOL_VERSION: &str = "poptart/0.1.0";
pub const AGENT_VERSION: &str = concat!("poptart/", env!("CARGO_PKG_VERSION"));

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event", event_process = false)]
pub struct PopTartBehaviour<S: Store> {
    relay_client: Toggle<RelayClient>,
    relay: Toggle<Relay>,
    identify: Identify,
    dcutr: Toggle<Dcutr>,
    pub bitswap: Bitswap<S>,
}

unsafe impl<S: Store> Send for PopTartBehaviour<S> {}
unsafe impl<S: Store> Sync for PopTartBehaviour<S> {}

impl<S: Store> PopTartBehaviour<S> {
    pub async fn new(
        keys: &Keypair,
        store: S,
        is_relay_client: bool,
        relay_client: Option<relay::v2::client::Client>,
    ) -> Self {
        let peer_id = keys.public().to_peer_id();
        let (dcutr, relay_client) = if is_relay_client {
            let relay_client =
                relay_client.expect("missing relay client even though it was enabled");
            let dcutr = Dcutr::new();
            (Some(dcutr), Some(relay_client))
        } else {
            (None, None)
        };

        let relay = if !is_relay_client {
            let config = relay::v2::relay::Config::default();
            let r = Relay::new(peer_id, config);
            Some(r)
        } else {
            None
        }
        .into();

        let identify = {
            let config = IdentifyConfig::new(PROTOCOL_VERSION.into(), keys.public())
                .with_agent_version(String::from(AGENT_VERSION))
                .with_cache_size(64 * 1024);
            Identify::new(config)
        };

        PopTartBehaviour {
            dcutr: dcutr.into(),
            identify,
            relay_client: relay_client.into(),
            relay,
            bitswap: Bitswap::new(peer_id, store.clone(), BitswapConfig::default()).await,
        }
    }
}

#[derive(Debug)]
pub enum Event {
    Bitswap(BitswapEvent),
    RelayClient(RelayClientEvent),
    Relay(RelayEvent),
    Dcutr(DcutrEvent),
    Identify(IdentifyEvent),
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

impl From<DcutrEvent> for Event {
    fn from(event: DcutrEvent) -> Self {
        Event::Dcutr(event)
    }
}

impl From<IdentifyEvent> for Event {
    fn from(event: IdentifyEvent) -> Self {
        Event::Identify(event)
    }
}
