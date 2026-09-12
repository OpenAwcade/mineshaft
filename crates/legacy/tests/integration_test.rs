use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use mineshaft_legacy::prelude::*;

#[tokio::test]
async fn test_full_mineshaft_end_to_end_flow() {
    // 1. Setup Mock Relay Server
    let mock_relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = mock_relay.local_addr().unwrap();

    // 2. Setup Mock Minecraft Bedrock Client Socket (LAN listener / player)
    let mc_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mc_addr = mc_client.local_addr().unwrap();

    // 3. Initialize Mineshaft Bridge for our local user (ClientId: 777)
    let bridge = Arc::new(MineshaftBridge::new(777, relay_addr, mc_addr).await.unwrap());

    // 4. Friend 1 (Ahmet), Friend 2 (Mehmet), and Friend 3 (Ayşe) open worlds
    let s1 = bridge.add_server(
        1001,
        888001,
        relay_addr,
        "MCPE;Ahmet's World;776;1.21.60;1;5;888001;Sub;Survival;1;19132;19133;",
    ).await.unwrap();

    let s2 = bridge.add_server(
        1002,
        888002,
        relay_addr,
        "MCPE;Mehmet's World;776;1.21.60;3;8;888002;Sub;Survival;1;19132;19133;",
    ).await.unwrap();

    let s3 = bridge.add_server(
        1003,
        888003,
        relay_addr,
        "MCPE;Ayşe's World;776;1.21.60;2;10;888003;Sub;Creative;2;19132;19133;",
    ).await.unwrap();

    // Verify all 3 servers got unique ephemeral ports
    assert_ne!(s1.ephemeral_port, s2.ephemeral_port);
    assert_ne!(s2.ephemeral_port, s3.ephemeral_port);
    assert_ne!(s1.ephemeral_port, s3.ephemeral_port);
    assert_eq!(bridge.server_manager.count().await, 3);

    // 5. Minecraft opens Friends tab -> Pong rain sent to mc_addr
    let sent_count = bridge.pong_engine.send_pong_burst_to(mc_addr).await;
    assert_eq!(sent_count, 3);

    // Collect and verify the 3 synthetic pongs received by Minecraft
    let mut received_servers = Vec::new();
    let mut buf = [0u8; 1024];

    for _ in 0..3 {
        let (len, _) = mc_client.recv_from(&mut buf).await.unwrap();
        let (_ts, guid, ident_str) = parse_unconnected_pong(&buf[..len]).unwrap();
        let ident = BedrockIdentifier::parse(&ident_str).unwrap();
        received_servers.push((guid, ident.server_name, ident.port_ipv4));
    }

    // Verify each world has distinct GUID and points to its own ephemeral port
    assert!(received_servers.iter().any(|(g, name, port)| *g == 888001 && name == "Ahmet's World" && *port == s1.ephemeral_port));
    assert!(received_servers.iter().any(|(g, name, port)| *g == 888002 && name == "Mehmet's World" && *port == s2.ephemeral_port));
    assert!(received_servers.iter().any(|(g, name, port)| *g == 888003 && name == "Ayşe's World" && *port == s3.ephemeral_port));

    // Spawn bridge background tasks for live packet routing
    bridge.spawn_background_tasks();

    // 6. User clicks Mehmet's World (ephemeral port s2) -> Minecraft sends Open Connection Request
    let target_mehmet = SocketAddr::new("127.0.0.1".parse().unwrap(), s2.ephemeral_port);
    let sample_raknet_packet = b"RakNet OpenConnectionRequest #1";
    mc_client.send_to(sample_raknet_packet, target_mehmet).await.unwrap();

    // 7. Receive from relay until we get the forwarded packet (ignoring initial [777, 777] handshake if present)
    let mut relay_buf = [0u8; 2048];
    let (header, payload, relay_src) = loop {
        let (relay_len, src) = mock_relay.recv_from(&mut relay_buf).await.unwrap();
        let (h, p) = MineshaftHeader::decode(&relay_buf[..relay_len]).unwrap();
        if !p.is_empty() {
            break (h, p, src);
        }
    };

    assert_eq!(header.target_session_id, 1002);
    assert_eq!(header.sender_session_id, 777);
    assert_eq!(payload, sample_raknet_packet);

    // 8. Mehmet's host replies through relay: [ 777 (Me), 1002 (Mehmet) ] + OpenConnectionReply
    let sample_reply = b"RakNet OpenConnectionReply #1";
    let reply_header = MineshaftHeader::new(777, 1002);
    let reply_pkt = reply_header.encapsulate(sample_reply);
    mock_relay.send_to(&reply_pkt, relay_src).await.unwrap();

    // 9. Verify Minecraft client receives the raw reply from its ephemeral socket!
    let mut mc_buf = [0u8; 1024];
    let recv_result = tokio::time::timeout(Duration::from_millis(500), mc_client.recv_from(&mut mc_buf)).await;
    assert!(recv_result.is_ok(), "Minecraft client should receive server reply");
    let (mc_len, from_addr) = recv_result.unwrap().unwrap();
    assert_eq!(&mc_buf[..mc_len], sample_reply);
    assert_eq!(from_addr.port(), s2.ephemeral_port);

    // 10. Test removing Ahmet's server (Ahmet closes his game / stops stream)
    let removed = bridge.remove_server(1001).await;
    assert!(removed.is_some());
    assert_eq!(bridge.server_manager.count().await, 2);

    // Verify next pong burst only sends 2 worlds (Mehmet and Ayşe)
    let sent_after_remove = bridge.pong_engine.send_pong_burst_to(mc_addr).await;
    assert_eq!(sent_after_remove, 2);

    bridge.stop();
}
