  ### High-Level Overview



  Rather than relying on unreliable direct NAT hole-punching (STUN/ICE), Omlet Arcade uses
  a Relay-Mediated Virtual LAN P2P architecture:

  - To the Minecraft game, the session appears as an ordinary local LAN multiplayer game.
  - Under the hood, mineshaft.node intercepts RakNet UDP datagrams, encapsulates them with
    custom 8-byte session routing headers, and tunnels them across Omlet Arcade’s low-
    latency UDP relay network.

  ———

  ### Architecture and Core Components

  #### 1. Local Game Discovery (Server Detector)

  - Scanning the Local Host: mineshaft.node creates a dedicated UDP socket
    (ServerDetector) and broadcasts RakNet ID_UNCONNECTED_PING (0x01) packets to UDP port
    19132 (Minecraft Bedrock's default port).

  - Packet Structure: The probe uses the standard 25-byte RakNet format:
      - Byte 0: 0x01 (ID_UNCONNECTED_PING)
      - Bytes 1–8: 64-bit client timestamp
      - Bytes 9–24: RakNet offline message ID magic (00 ff ff 00 fe fe fe fe fd fd fd fd
        12 34 56 78)

  - Detecting Local Worlds: When the user opens a world in Minecraft, the game replies
    with ID_UNCONNECTED_PONG (0x1C).

  - Parsing the Identifier: mineshaft.node verifies the magic bytes and reads the Bedrock
    server identifier string
    (MCPE;WorldName;Protocol;Version;Players;MaxPlayers;GUID;...), firing
    localServerRunning or localServerRunningNoMultiPlayer events back to Node.js.

  #### 2. Loopback Bypass (retroflect.exe + WinDivert)

  - UWP AppContainer Isolation: On Windows 10, Minecraft Bedrock runs inside an
    AppContainer sandbox, which by default blocks UWP apps from connecting to 127.0.0.1
    (loopback isolation).

  - Packet Reflection: Omlet Arcade bundles retroflect/retroflect.exe alongside the
    WinDivert kernel driver (WinDivert64.sys).

  - When initialized, Omlet Arcade selects an unused virtual IP (e.g., 10.10.10.10) via
    setReflectorAddress. retroflect.exe intercepts outbound traffic destined for this IP
    and redirects it back into the local stack, allowing Minecraft to communicate with
    mineshaft.node without requiring manual Windows loopback exemption utilities.

  #### 3. Presence and Relay Assignment

  - Relay Allocation: When Omlet Arcade connects, it calls the backend endpoint
    LDGetMinecraftRelayInfoRequest, which returns:
      - RelayServerAndPort (e.g. relay-sg.omlet.me:<port>)
      - ClientId (a 32-bit unique tunnel identifier)

  - Relay Handshake: mineshaft.node creates a UDP socket connected to the relay server and
    immediately sends an 8-byte handshake:

    [ 32-bit ClientId, 32-bit ClientId ]

  - Keep-Alives: A 25-second keep-alive timer repeatedly sends this 8-byte packet to keep
    the client's router NAT mapping open.

  - Backend Presence: If the host is in a multiplayer world, the Electron app publishes
    presence to Omlet's cloud (LDSetOnlineStatusRequest) containing:
      - Host's ClientId
      - Host's MCPERelayAddress (IP:port)
      - Host's MCPEServerRakNetId
      - Base64-encoded Minecraft server identifier string

  #### 4. Remote World Advertising (Virtual LAN Injection)

  - Adding Remote Friends: When friends or followed users host games, the client receives
    their presence and calls mineshaft.addServer(targetClientId, targetRakNetId, relayIp,
    relayPort, identifier, true).

  - Ephemeral Client Socket: For each remote world, mineshaft.node binds a local UDP
    socket on an ephemeral OS-assigned port.

  - Synthesizing PONGs: When the local player opens the Minecraft "Friends" tab, Minecraft
    sends a LAN broadcast ping. mineshaft.node intercepts this and responds with a crafted
    ID_UNCONNECTED_PONG (0x1C):
      - It inserts the remote host's world metadata and world name.
      - Crucially, it replaces the server port with the local ephemeral socket port.
      - To Minecraft, the remote friend's game appears directly in the local "LAN Games"
        list.

  #### 5. Datagram Tunneling and Packet Rewriting

  - Outgoing Client Flow:
      1. Local Minecraft sends RakNet connection datagrams (0x05 Open Connection Request
         1, 0x07 Open Connection Request 2, 0x84 Datagrams) to the local ephemeral port.

      2. mineshaft.node receives the packet and invokes an internal packet patcher
         (0x180007720) to rewrite internal RakNet connection ports to match expected local
         endpoints.

      3. It prepends an 8-byte routing header containing:

         [ 32-bit Destination ClientId, 32-bit Sender ClientId ]

      4. It sends the encapsulated datagram over UDP to the Omlet Relay server.

  - Incoming Host Flow:
      1. The host's mineshaft.node receives the datagram from its relay socket.
      2. It inspects the first 4 bytes to identify the connecting peer (ClientId).
      3. It verifies access control (checks if the player is in blockedPlayers or passes
         multiPlayerFollowOnly checks).

      4. It triggers clientConnecting to Node.js so Omlet Arcade can fetch the player's
         profile avatar and gamertag.

      5. It strips the 8-byte header and forwards the raw RakNet packet to the local
         Minecraft instance listening on port 19132.

  - Return Path: Outgoing replies from the host's Minecraft server are caught by
    mineshaft.node, tagged with the peer's ClientId, sent back through the relay, and
    forwarded to the client's local ephemeral socket into their Minecraft game.

  #### 6. Player Lifecycle and Moderation

  - Active Tracking: mineshaft.node tracks active UDP sessions and periodically emits
    playersActive (a binary buffer of active 64-bit client identifiers) to JavaScript.

  - Kicking and Blocking: If the host toggles multiplayer off, activates "Followers Only",
    or clicks "Kick", the JS layer calls blockPlayer(clientId). Any incoming relay packets
    matching that ClientId are dropped at the native C++ level.

  ———

  ### Summary of P2P Architecture

   Step                   Mechanism
  ━━━━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   Local Detection        RakNet 0x01 ping on port 19132; parses 0x1C pong to capture
                          game version and world name.
  ─────────────────────  ─────────────────────────────────────────────────────────────────
   Windows Loopback       retroflect.exe via WinDivert routes 10.10.10.10 packets past
                          UWP AppContainer loopback isolation.
  ─────────────────────  ─────────────────────────────────────────────────────────────────
   P2P Transport          Low-latency UDP relay mesh; peers do not need port forwarding
                          or direct public IPs.
  ─────────────────────  ─────────────────────────────────────────────────────────────────
   Session Framing        8-byte header [ TargetClientId, SourceClientId ] prepended to
                          standard RakNet datagrams.
  ─────────────────────  ─────────────────────────────────────────────────────────────────
   Client Presentation    Injects synthetic LAN ID_UNCONNECTED_PONG so remote friends
                          show up natively in the Minecraft LAN list.

• The function initializes a UDP port fetcher only if one doesn't already exist. It then
  uses a tool with specific arguments to retrieve UDP port information. The output is
  processed line by line, filtering for lines starting with "UDP". Upon exit, it nullifies
  the fetcher.

  Determining UDP Port Activity

  The code uses a command-line utility to identify active UDP ports on the system. It
  filters the output to only include UDP-related entries. The goal is likely to detect
  which ports are currently in use, potentially to prevent conflicts or to understand
  network activity. The data is then likely passed to a function that handles updating
  port information within the application.

  Analyzing Network Connectivity Methods

  The UWP environment's loopback restrictions necessitate a workaround; the system uses a
  driver with a custom virtual address to redirect UDP traffic. Instead of direct P2P
  hole-punching, it relies on a relay-mediated model, crucial given NAT complexities, and
  the back-end assigns clients a unique ID and relay server. The node then connects to the
  relay and exchanges handshake data, maintaining the connection through keep-alives.

  Friend world joining relies on the backend sharing presence information, and for each
  available world, a method is called. The node binds a UDP socket to an ephemeral port
  and, when a client pings for LAN worlds, injects a synthetic identifier into the traffic
  stream.