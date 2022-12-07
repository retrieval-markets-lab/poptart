use libp2p::relay::v2::client::Client as RelayClient;
use libp2p::{
    core::{self, muxing::StreamMuxerBox, transport::Boxed, transport::OrTransport},
    dns,
    identity::Keypair,
    noise, quic,
    swarm::derive_prelude::EitherOutput,
    tcp, websocket,
    yamux::{self, WindowUpdateMode},
    PeerId, Transport,
};

/// Builds the transport stack that LibP2P will communicate over.
pub async fn build_transport(
    keys: &Keypair,
    is_relay_client: bool,
) -> (Boxed<(PeerId, StreamMuxerBox)>, Option<RelayClient>) {
    // TCP
    let tcp_config = tcp::Config::default().port_reuse(true);
    let tcp_transport = tcp::tokio::Transport::new(tcp_config.clone());
    // Websockets
    let ws_tcp = websocket::WsConfig::new(tcp::tokio::Transport::new(tcp_config));
    let tcp_ws_transport = tcp_transport.or_transport(ws_tcp);
    // Quic
    let quic_config = quic::Config::new(&keys);
    let quic_transport = quic::tokio::Transport::new(quic_config);
    // Noise config for TCP & Websockets
    let auth_config = {
        let dh_keys = noise::Keypair::<noise::X25519Spec>::new()
            .into_authentic(&keys)
            .expect("Noise key generation failed");
        noise::NoiseConfig::xx(dh_keys).into_authenticated()
    };
    // Stream muxer config for TCP & Websockets
    let mut yamux_config = yamux::YamuxConfig::default();
    yamux_config.set_max_buffer_size(16 * 1024 * 1024);
    yamux_config.set_receive_window_size(16 * 1024 * 1024);
    yamux_config.set_window_update_mode(WindowUpdateMode::on_receive());

    // Websockets

    let (tcp_ws_transport, relay_client) = if is_relay_client {
        let (relay_transport, relay_client) =
            RelayClient::new_transport_and_behaviour(keys.public().to_peer_id());

        let transport = OrTransport::new(relay_transport, tcp_ws_transport);
        let transport = transport
            .upgrade(core::upgrade::Version::V1Lazy)
            .authenticate(auth_config)
            .multiplex(yamux_config)
            .boxed();

        (transport, Some(relay_client))
    } else {
        let tcp_transport = tcp_ws_transport
            .upgrade(core::upgrade::Version::V1Lazy)
            .authenticate(auth_config)
            .multiplex(yamux_config)
            .boxed();

        (tcp_transport, None)
    };

    let transport = OrTransport::new(quic_transport, tcp_ws_transport)
        .map(|o, _| match o {
            EitherOutput::First((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            EitherOutput::Second((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    // DNS

    let dns_cfg = dns::ResolverConfig::cloudflare();
    let dns_opts = dns::ResolverOpts::default();
    let transport = dns::TokioDnsConfig::custom(transport, dns_cfg, dns_opts)
        .unwrap()
        .boxed();

    (transport, relay_client)
}
