"""Unit tests for the failure detector."""

import time
from meshweaver.core.state import PeerHealth
from meshweaver.reliability.failure_detector import FailureDetector


def test_failure_detector_transitions():
    fd = FailureDetector(suspect_timeout=0.1, dead_timeout=0.25)
    peer_id = "test-peer"

    # Initially unknown peer is considered DEAD
    assert fd.get_health(peer_id) == PeerHealth.DEAD

    # Heartbeat arrives -> HEALTHY
    fd.record_heartbeat(peer_id)
    assert fd.get_health(peer_id) == PeerHealth.HEALTHY

    # Wait past suspect_timeout
    time.sleep(0.12)
    assert fd.get_health(peer_id) == PeerHealth.SUSPECT

    # Wait past dead_timeout
    time.sleep(0.15)
    assert fd.get_health(peer_id) == PeerHealth.DEAD

    # New heartbeat arrives -> revives to HEALTHY
    fd.record_heartbeat(peer_id)
    assert fd.get_health(peer_id) == PeerHealth.HEALTHY


def test_peer_table_indirect_gossip_does_not_reset_liveness():
    from meshweaver.core.state import NodeInfo, PeerHealth, ResourceMetrics
    from meshweaver.gossip.peer_state import PeerTable

    table = PeerTable()
    info = NodeInfo(node_id="peer-123")
    peer = table.update_peer(info, direct_contact=True)
    peer.health = PeerHealth.SUSPECT
    original_last_seen = 100.0
    peer.last_seen = original_last_seen

    # Indirect gossip update should update metrics but NOT reset health or last_seen
    new_metrics = ResourceMetrics(cpu_percent=45.0, timestamp=200.0)
    table.update_peer(info, metrics=new_metrics, direct_contact=False)

    assert peer.metrics.cpu_percent == 45.0
    assert peer.health == PeerHealth.SUSPECT
    assert peer.last_seen == original_last_seen

