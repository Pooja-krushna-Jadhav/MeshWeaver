"""Unit tests for Kademlia DHT RPC handling."""

import pytest
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.routing_table import Contact
from meshweaver.networking.protocol import Message
from meshweaver.utils.helpers import generate_node_id


@pytest.mark.asyncio
async def test_kademlia_handle_ping_and_store():
    local_id = generate_node_id("local")
    remote_id = generate_node_id("remote")

    async def dummy_send_rpc(h, p, m, t):
        return None

    kad = KademliaNode(
        local_node_id=local_id,
        host="127.0.0.1",
        tcp_port=8000,
        udp_port=8001,
        send_rpc_fn=dummy_send_rpc,
    )

    # 1. Test PING RPC
    contact_data = Contact(remote_id, "127.0.0.1", 9000, 9001).to_dict()
    ping_msg = Message(
        sender_id=remote_id,
        msg_type="kad_rpc",
        payload={"rpc": "PING", "contact": contact_data},
    )
    reply = await kad.handle_rpc(ping_msg)
    assert reply.payload["rpc"] == "PONG"
    # Remote peer should be recorded in routing table
    assert kad.routing_table.get_contact(remote_id) is not None

    # 2. Test STORE RPC
    store_msg = Message(
        sender_id=remote_id,
        msg_type="kad_rpc",
        payload={"rpc": "STORE", "key": "my_dataset", "value": {"size": 1024}},
    )
    store_reply = await kad.handle_rpc(store_msg)
    assert store_reply.payload["rpc"] == "STORE_ACK"
    assert "my_dataset" in kad.storage
    assert kad.storage["my_dataset"][0] == {"size": 1024}

    # 3. Test FIND_VALUE RPC
    find_msg = Message(
        sender_id=remote_id,
        msg_type="kad_rpc",
        payload={"rpc": "FIND_VALUE", "key": "my_dataset"},
    )
    find_reply = await kad.handle_rpc(find_msg)
    assert find_reply.payload["rpc"] == "FIND_VALUE_RESP"
    assert find_reply.payload["found"] is True
    assert find_reply.payload["value"] == {"size": 1024}


@pytest.mark.asyncio
async def test_kademlia_bootstrap_discovers_contact():
    local_id = generate_node_id("local")
    remote_id = generate_node_id("remote")

    async def mock_send_rpc(h, p, m, t):
        if m.payload.get("rpc") == "PING":
            return Message(
                sender_id=remote_id,
                msg_type="kad_rpc",
                payload={"rpc": "PONG", "contact": Contact(remote_id, h, p, p + 1).to_dict()},
            )
        elif m.payload.get("rpc") == "FIND_NODE":
            return Message(
                sender_id=remote_id,
                msg_type="kad_rpc",
                payload={"rpc": "FIND_NODE_RESP", "contacts": [Contact(remote_id, h, p, p + 1).to_dict()]},
            )
        return None

    kad = KademliaNode(
        local_node_id=local_id,
        host="127.0.0.1",
        tcp_port=8000,
        udp_port=8001,
        send_rpc_fn=mock_send_rpc,
    )

    seed_contact = Contact(
        node_id=generate_node_id("seed_placeholder"),
        host="127.0.0.1",
        tcp_port=9000,
        udp_port=9001,
    )

    success_count = await kad.bootstrap([seed_contact])
    assert success_count == 1
    assert kad.routing_table.get_contact(remote_id) is not None
