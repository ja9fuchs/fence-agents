# fence_site - Site-Wide Parallel Fencing Agent

## Overview

`fence_site` is a fence agent that enables **synchronous site-wide fencing** in Pacemaker clusters. When a node fails, fence_site coordinates parallel fencing of all nodes at the same physical site, dramatically reducing recovery time during site-level failures.

Unlike traditional fence agents that power off nodes directly, fence_site works by:
1. Identifying peer nodes at the same site (via cluster attributes)
2. Setting `terminate` attributes on eligible peers
3. Triggering Pacemaker to fence them in parallel using real fence devices

**Critical**: fence_site must be configured **with** a real fence device on the same topology level and must be **first** in the device list.

---

## How It Works

```text
Site A Fails (node1 down)
         ↓
    Pacemaker initiates fencing
         ↓
    fence_site (runs first)
         ↓
    Identifies peers: node2, node3 on site=siteA
         ↓
    Checks peer uptime (default: 15min minimum)
         ↓
    Validates quorum safety
         ↓
    Sets terminate=true on node1, node2, node3
         ↓
    Real fence device fences all nodes (target first, then peers in a batch - or all nodes parallel with force_reschedule)
         ↓
    Site recovery complete (node4, node5 on siteB remain operational)
```

### Key Concepts

- **Site Attribute**: Cluster node attribute defining site membership (default: `site`)
- **Uptime Threshold**: Minimum time peer must be online before fencing (default: 900s)
  - Prevents fencing nodes during cluster startup
  - Set to `0` to fence all peers immediately regardless of uptime
- **Quorum Safety**: Prevents peer fencing if would cause quorum loss (default: enabled)
- **Force Reschedule**: Optional mode to fence target + peers simultaneously (default: disabled)

---

## Use Cases

- **Site-level failures** in geo-distributed clusters
- **Rack isolation** in multi-rack data centers  
- **Zone failures** in cloud deployments
- **Network partition** with site-wide connectivity loss
- **Disaster recovery** scenarios requiring fast site failover

### When NOT to Use

- Single-site clusters (no benefit over traditional fencing)
- Sites with only one node (use traditional fence agent)
- Clusters without site attributes configured

---

## Configuration

### Prerequisites

1. Cluster nodes have site attribute configured:
```bash
pcs node attribute node1 set site siteA
pcs node attribute node2 set site siteA
pcs node attribute node3 set site siteA
pcs node attribute node4 set site siteB
pcs node attribute node5 set site siteB
```

2. Real fence devices configured, for example, one device that handles all nodes:
```bash
pcs stonith create <real-fence-device> <fence-agent> \
    pcmk_host_list="node1 node2 node3 node4 node5"
    ...
```

### Basic Setup

Create fence_site resource and configure topology:

```bash
# Create fence_site resource
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site

# Add to fencing topology (fence_site FIRST, then real device)
pcs stonith level add 1 node1 fence-site <real-fence-device>
pcs stonith level add 1 node2 fence-site <real-fence-device>  
pcs stonith level add 1 node3 fence-site <real-fence-device>
pcs stonith level add 1 node4 fence-site <real-fence-device>
pcs stonith level add 1 node5 fence-site <real-fence-device>
```

### Advanced Setup: Custom Uptime Threshold

Adjust uptime threshold for your environment:

```bash
# Require 30 minutes uptime before peer fencing
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site \
    uptime_threshold=1800

# Disable uptime check (fence all peers immediately)
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site \
    uptime_threshold=0
```

### Advanced Setup: Force Parallel Fencing

Fence target and all peers simultaneously (not sequentially):

```bash
# Create fence_site with force_reschedule
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site \
    force_reschedule=true \
    pcmk_off_retries=0

# Update the real fence device when it handles multiple nodes
pcs stonith update <real-fence-device> pcmk_action_limit=-1
```

### Advanced Setup: Disable Quorum Safety

For disaster recovery scenarios where site must be fenced regardless of quorum:

```bash
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site \
    quorum_safe=false
```

**⚠️ WARNING**: Disabling quorum safety can cause cluster-wide outage. Only use in controlled scenarios.

---

## Options

### Site Fencing Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--site-attribute` | `site` | Node attribute defining site membership |
| `--uptime-threshold` | `900` | Minimum peer uptime (seconds) before fencing<br>Set to `0` to skip uptime check |
| `--quorum-safe` | `true` | Prevent peer fencing if would lose quorum |
| `--force-reschedule` | `false` | Force parallel fencing by failing when peers triggered<br>Requires `pcmk_off_retries=0` |
| `--dry-run` | `false` | Log actions without executing (testing only) |

### Standard Fence Options

| Option | Default | Description |
| ------ | ------- | ----------- |
| `--plug` | (required) | Target node name to fence |
| `--action` | `off` | Fence action: `off`, `on` (unfence), `reboot` |
| `--power-timeout` | `60` | Fence operation timeout (seconds) |
| `--shell-timeout` | `30` | Shell command timeout (seconds) |
| `-v, --verbose` | (disabled) | Enable debug logging |

### Pacemaker Integration Options

| Option | Description |
| ------ | ----------- |
| `pcmk_host_list` | Space-separated list of nodes this device can fence |
| `pcmk_off_retries` | Set to `0` when using `force_reschedule` |
| `pcmk_action_limit` | Set to `-1` on real devices for parallel fencing, when the real device handles more than 1 node |

---

## Actions

### off / reboot

**Primary fencing action** - identifies and fences peer nodes at the same site.

**Workflow**:
1. Validate target is cluster member
2. Validate fencing topology configuration
3. Query all node states and site attributes (batch)
4. Identify peer nodes on same site as target
5. Filter peers by uptime threshold (unless threshold=0)
6. Validate quorum safety (if enabled)
7. Set `terminate=true` on target and eligible peers
8. Return success/failure based on mode

**Return Values**:
- **Success (default mode)**: Terminate attributes set, Pacemaker proceeds to real fence device
- **Failure (force_reschedule mode)**: Intentional failure to trigger parallel reschedule

### on (Unfence)

**Unfencing action** - clears terminate attribute when node starts.

```bash
fence_site --plug node1 --action on
```

**Workflow**:
1. Delete `terminate` status attribute for node
2. Return success

**Purpose**: Clean up terminate attribute after node recovery.

### status / monitor

Not supported - fence_site has no power state to query.

---

## Examples

### Example 1: Basic Site Fencing

**Scenario**: 5-node cluster, 2 sites (siteA=node1,node2,node3; siteB=node4,node5)

```bash
# Configure site attributes
pcs node attribute node1 set site siteA
pcs node attribute node2 set site siteA
pcs node attribute node3 set site siteA
pcs node attribute node4 set site siteB
pcs node attribute node5 set site siteB

# Create fence_site
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site

# Configure topology
pcs stonith level add 1 node1 fence-site <real-fence-device>
pcs stonith level add 1 node2 fence-site <real-fence-device>
pcs stonith level add 1 node3 fence-site <real-fence-device>
pcs stonith level add 1 node4 fence-site <real-fence-device>
pcs stonith level add 1 node5 fence-site <real-fence-device>

# Test manually (dry-run)
fence_site --plug node1 --action off --dry-run -v
```

**Result**: When node1 fails, node2 and node3 get `terminate=true` and all three are fenced in parallel. node4 and node5 (siteB) remain operational.

### Example 2: Zero Uptime Threshold for Disaster Recovery

**Scenario**: Site failure during cluster startup, need immediate site-wide fencing

```bash
# Create fence_site with no uptime requirement
pcs stonith create fence-site fence_site \
    pcmk_host_list="node1 node2 node3 node4 node5" \
    site_attribute=site \
    uptime_threshold=0
```

**Result**: All peers at failed site are fenced immediately, regardless of how long they've been online. When node1 fails, node2 and node3 are fenced even if they just joined the cluster.

### Example 3: Testing with Verbose Logging

```bash
# Enable debug logging to see peer identification
fence_site --plug node1 --action off \
    --site-attribute site \
    --uptime-threshold 900 \
    --dry-run -v

# Expected output (abbreviated):
# INFO: Target node1: Starting site-wide fencing
# INFO: Target node1: Target node is on site: siteA
# INFO: Target node1: Peer node2 eligible (uptime 2400s >= 900s)
# INFO: Target node1: Peer node3 eligible (uptime 1800s >= 900s)
# INFO: Target node1: Peer node4 on different site (siteB), skipping
# INFO: Target node1: Peer node5 on different site (siteB), skipping
# INFO: Target node1: 2 peers eligible for fencing
# INFO: Target node1: Quorum check: SAFE
# INFO: DRY-RUN: Would execute: crm_attribute --node node1 --name terminate --update true --type status
# INFO: DRY-RUN: Would execute: crm_attribute --node node2 --name terminate --update true --type status
# INFO: DRY-RUN: Would execute: crm_attribute --node node3 --name terminate --update true --type status
```

### Example 4: Manual Unfencing

```bash
# Clear terminate attribute after manual recovery
fence_site --plug node1 --action on

# Verify attribute cleared
crm_attribute --node node1 --name terminate --query --type status
# Expected: Error performing operation: No such device or address
```

### Example 5: Topology Validation Error

```bash
# INCORRECT: fence_site alone on level (will fail validation)
pcs stonith level add 1 node1 fence-site

# Test it
fence_site --plug node1 --action off -v

# Expected error:
# ERROR: Target node1: fence_site is ALONE at topology level 1
# ERROR: Target node1: fence_site MUST be configured with a real fence device
```

---

## Safety Features

### 1. Uptime Threshold Protection

**Problem**: Fencing nodes during cluster startup can cause cascading failures.

**Solution**: Only fence peers online for minimum duration (default 15 minutes).

```bash
# Node startup timeline (siteA recovery scenario):
# T+0:00 - node1, node2 online (uptime 60min)
# T+0:00 - node3 joins cluster (in_ccm timestamp recorded, uptime 0min)
# T+05:00 - node1 fails
# T+05:01 - fence_site runs:
#   - node2 uptime = 65min > 900s threshold ✓ eligible
#   - node3 uptime = 5min < 900s threshold ✗ skipped
# Result: node1 and node2 fenced, node3 NOT fenced (too new)
# node3 remains online
```

**Override**: Set `uptime_threshold=0` to fence all peers regardless of uptime. Make sure to use the "off" action for fencing, instead of "reboot", to prevent possible reboot loops.

#### 1.1 in_ccm="true" Edge Case Handling

**Problem**: Older Pacemaker versions may show `in_ccm="true"` instead of timestamp (CRM feature set <3.18.0).

**Solution**: When `uptime_threshold=0`, skip uptime check entirely (processes these nodes). Make sure to use the "off" action for fencing, instead of "reboot", to prevent possible reboot loops. When `uptime_threshold>0`, treat as unavailable (skip these nodes). 

### 2. Quorum Safety Check

**Problem**: Fencing entire site could cause quorum loss.

**Solution**: Validate remaining nodes meet quorum threshold before peer fencing.

```bash
# 5-node cluster: expected_votes=5, quorum=3
# siteA has 3 nodes (node1, node2, node3)
# siteB has 2 nodes (node4, node5)
# siteA fails (node1 down)
# Peers at siteA: node2, node3
# Calculation: 5 nodes - 3 fenced = 2 remaining < 3 quorum
# Result: Peers NOT fenced (quorum safety), only node1 fenced
# siteB nodes (node4, node5) maintain cluster but lose quorum if siteA fenced
```

**Override**: Set `quorum_safe=false` for disaster recovery scenarios.

### 3. Topology Validation

**Problem**: Misconfigured topology can cause infinite loops or false fencing results that can lead to split brain situations.

**Solution**: Validate fence_site is properly configured before running.

**Checks**:
- ✓ fence_site is NOT the only device in topology
- ✓ fence_site is NOT alone on its level
- ✓ fence_site is FIRST device on its level

**Example failure**:
```bash
# BAD: fence_site after real device
pcs stonith level add 1 node1 <real-fence-device> fence-site
# fence_site runs AFTER fencing completes (useless)

# GOOD: fence_site before real device  
pcs stonith level add 1 node1 fence-site <real-fence-device>
# fence_site sets terminate, THEN real device fences
```

---

## Fencing Modes

### Default Mode: Sequential Target → Parallel Peers

**Behavior**:
1. fence_site sets terminate attributes
2. fence_site returns **success**
3. Pacemaker fences target with real device
4. Pacemaker sees peers with terminate=true
5. Pacemaker fences all peers in parallel

**Timeline**:
```
T+0s: node1 (siteA) fails
T+1s: fence_site identifies peers: node2, node3
T+2s: fence_site sets terminate on node1, node2, node3
T+3s: fence_site returns success
T+4s: <real-fence-device> fences node1 (complete)
T+5s: Pacemaker schedules node2, node3 fencing
T+6s: <real-fence-device> runs in parallel for node2 and node3
T+7s: All siteA nodes fenced (complete)
      node4, node5 (siteB) operational
```

**Use when**: Standard site fencing acceptable.

### Force Reschedule Mode: Fully Parallel

**Behavior**:
1. fence_site sets terminate attributes
2. fence_site returns **failure** (intentional)
3. Pacemaker reschedules ALL nodes together
4. Pacemaker fences target + peers in parallel

**Timeline**:
```
T+0s: node1 (siteA) fails
T+1s: fence_site identifies peers: node2, node3
T+2s: fence_site sets terminate on node1, node2, node3
T+3s: fence_site returns FAILURE (intentional)
T+4s: Pacemaker reschedules: node1 + node2 + node3 fencing
T+5s: <real-fence-device> runs in parallel for node1 + node2 + node3
T+6s: All siteA nodes fenced (complete)
      node4, node5 (siteB) operational
```

**Configuration**:
```bash
pcs stonith update fence-site force_reschedule=true pcmk_off_retries=0

# When the real fence device handles multiple nodes: 
pcs stonith update <real-fence-device> pcmk_action_limit=-1
```

**Use when**: Absolute minimum recovery time required.

---

## Troubleshooting

### Issue: Peers Not Being Fenced

**Symptoms**: Target fenced, but peers at same site remain online.

**Diagnosis**:
```bash
# Check site attributes
pcs node attribute

# Check peer uptime
crm_mon -A1  # Look for in_ccm timestamps

# Test manually with verbose logging
fence_site --plug node1 --action off -v --dry-run
```

**Common Causes**:
1. **Uptime below threshold**: Peers too new, filtered out
   - Solution: Lower `uptime_threshold` or set to `0`
2. **Quorum safety triggered**: Would lose quorum
   - Check logs for "Quorum safety check FAILED"
   - Solution: Adjust cluster size or set `quorum_safe=false`
3. **Site attribute mismatch**: Peers have different site value
   - Verify: `pcs node attribute | grep site`
4. **in_ccm="true" with threshold>0**: Peers skipped due to edge case
   - Solution: Set `uptime_threshold=0`

### Issue: Topology Validation Failures

**Symptoms**: 
```
ERROR: fence_site is ALONE at topology level 1
ERROR: fence_site MUST be configured with a real fence device
```

**Solution**:
```bash
# Check current topology
pcs stonith level

# Fix: Add fence_site WITH real device
pcs stonith level clear
pcs stonith level add 1 node1 fence-site <real-fence-device>
```

### Issue: Self-Fencing

**Symptoms**: fence_site tries to fence itself.

**Cause**: fence_site configured in `pcmk_host_list` but shouldn't self-fence.

**Solution**: This is normal - fence_site handles target node appropriately. No action needed.

### Issue: Force Reschedule Not Working

**Symptoms**: Target fenced first, then peers (not parallel).

**Diagnosis**:
```bash
# Check fence_site config
pcs stonith config fence-site | grep -E "force_reschedule|pcmk_off_retries"

# Check real device config
pcs stonith config <real-fence-device> | grep pcmk_action_limit
```

**Required Configuration**:
```bash
# fence_site must have BOTH
pcs stonith update fence-site \
    force_reschedule=true \
    pcmk_off_retries=0

# Real devices must allow parallel operations, if they handle
# more than one node
pcs stonith update <real-fence-device> pcmk_action_limit=-1
```

### Issue: in_ccm Timestamp Issues

**Symptoms**: Peers always skipped, logs show "uptime unavailable"

**Diagnosis**:
```bash
# Check Pacemaker feature set
cibadmin --query | grep validate-with
# Needs: pacemaker-2.1.7 (CRM feature set 3.18.0) or later for in_ccm timestamps

# Check node_state
cibadmin --query --xpath "//node_state[@uname='node2']"
# Look for: in_ccm="1234567890" (timestamp) or in_ccm="true" (edge case)
```

**Solution**:
- Upgrade to Pacemaker 2.1.7+ (CRM feature set 3.18.0)
- OR set `uptime_threshold=0` to bypass uptime checks

### Debug Logging

Enable comprehensive logging for troubleshooting:

```bash
# Test with verbose output
fence_site --plug node1 --action off -v --dry-run

# Check syslog for fence_site operations  
journalctl -t fence_site -f

# Check Pacemaker logs
journalctl -u pacemaker -f | grep -i fence
```

---

## Requirements

### Cluster Requirements
- **Pacemaker**: 2.1.7+ (CRM feature set 3.18.0) for uptime tracking
  - Earlier versions: Set `uptime_threshold=0` to bypass
- **Corosync**: Standard installation
- **Site attributes**: Configured on all nodes

### Topology Requirements
- fence_site must be on same level as real fence device
- fence_site must be FIRST device in the list
- Real fence device must follow fence_site

### System Requirements
- `/usr/share/fence` Python library (from fence-agents package)
- Python 3.6+
- Standard Pacemaker CIB tools: `cibadmin`, `crm_attribute`, `crm_node`

---

## Installation

```bash
# From fence-agents source
./autogen.sh
./configure
make
sudo make install

# Verify installation
fence_site -o metadata

# Check version
fence_site --version
```

---

## See Also

- `pcs stonith` - Pacemaker fence device configuration
- [Pacemaker Fencing Documentation](https://clusterlabs.org/pacemaker/doc/2.1/Pacemaker_Explained/html/fencing.html)
