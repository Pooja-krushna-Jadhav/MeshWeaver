"""Unit tests for Kademlia routing table and XOR distance."""

from meshweaver.discovery.routing_table import Contact, RoutingTable
from meshweaver.utils.helpers import generate_node_id, xor_distance


def test_xor_distance():
    id1 = "0000000000000000000000000000000000000001"
    id2 = "0000000000000000000000000000000000000003"
    dist = xor_distance(id1, id2)
    assert dist == 2


def test_routing_table_add_and_find_closest():
    local_id = generate_node_id("local")
    rt = RoutingTable(local_id, k_size=5)

    contacts = [
        Contact(generate_node_id(f"peer_{i}"), "127.0.0.1", 8000 + i, 9000 + i)
        for i in range(10)
    ]

    for c in contacts:
        rt.add_contact(c)

    # Search closest to a target
    target = generate_node_id("target")
    closest = rt.find_closest_contacts(target, count=3)
    assert len(closest) == 3

    # Ensure returned contacts are sorted by XOR distance to target
    dists = [xor_distance(c.node_id, target) for c in closest]
    assert dists == sorted(dists)


def test_routing_table_remove():
    local_id = generate_node_id("local")
    rt = RoutingTable(local_id)
    c = Contact(generate_node_id("target"), "127.0.0.1", 8080, 8081)

    rt.add_contact(c)
    assert rt.get_contact(c.node_id) is not None

    rt.remove_contact(c.node_id)
    assert rt.get_contact(c.node_id) is None


def test_kbucket_replacement_cache_promotion():
    from meshweaver.discovery.routing_table import KBucket

    bucket = KBucket(k_size=2)
    c1 = Contact("1" * 40, "127.0.0.1", 8001, 9001)
    c2 = Contact("2" * 40, "127.0.0.1", 8002, 9002)
    c3 = Contact("3" * 40, "127.0.0.1", 8003, 9003)

    assert bucket.add(c1) is True
    assert bucket.add(c2) is True
    # Bucket is full; c3 goes into replacement cache
    assert bucket.add(c3) is False
    assert len(bucket) == 2
    assert bucket.head().node_id == c1.node_id

    # Remove c1 -> c3 is promoted from replacement cache into bucket
    assert bucket.remove(c1.node_id) is True
    assert len(bucket) == 2
    assert bucket.get_contact(c3.node_id) is not None
    assert bucket.get_contact(c2.node_id) is not None

