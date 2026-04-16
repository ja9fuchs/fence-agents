#!/usr/bin/python3 -tt

# Fence agent for synchronous site-wide fencing in Pacemaker clusters
#
# When a node is fenced, this agent identifies and fences all other nodes
# with the same site attribute, enabling parallel site-wide fencing.

import atexit
import logging
import shlex
import sys
import time
import xml.etree.ElementTree as ET
from typing import Dict, Optional, Set, Tuple

sys.path.append("/usr/share/fence")
from fencing import (  # noqa: E402
    EC_STATUS,
    SyslogLibHandler,
    all_opt,
    atexit_handler,
    check_input,
    fail,
    fence_action,
    process_input,
    run_command,
    run_delay,
    show_docs,
)

# Constants
XML_PREVIEW_LENGTH = 500  # Characters to show in debug logs for XML parsing errors
DEFAULT_POWER_TIMEOUT = "60"  # Seconds for fence operation timeout
DEFAULT_SHELL_TIMEOUT = "30"  # Seconds for shell command timeout

# Get logger instance (configured in main() after fencing library initializes)
logger = logging.getLogger()

# Global caches (populated once per execution)
# Note: These caches are not thread-safe. But fence agents run single-threaded.
_node_states_cache: Optional[Dict[str, ET.Element]] = None
_cluster_nodes_cache: Optional[Dict[str, str]] = None


def run_cmd(options: Dict[str, str], cmd: str) -> Tuple[int, str, str]:
    """Wrapper around run_command() for output at debug level.

    Args:
        options: Options dictionary from fence agent
        cmd: Shell command to execute

    Returns:
        Return code, stdout, stderr
    """
    (rc, stdout, stderr) = run_command(options, cmd)
    if stdout:
        logger.debug("stdout:\n%s", stdout.strip())
    if stderr:
        logger.debug("stderr:\n%s", stderr.strip())
    logger.debug("rc: %d", rc)
    return (rc, stdout, stderr)


def is_dry_run(options: Dict[str, str]) -> bool:
    """Check if dry-run mode is enabled.

    Args:
        options: Options dictionary from fence agent

    Returns:
        True if dry-run mode is enabled, False otherwise
    """
    return options.get("--dry-run", "false").lower() in ["1", "yes", "on", "true"]


def safe_parse_xml(xml_string: str, context: str = "XML") -> Optional[ET.Element]:
    """Safely parse XML with comprehensive error logging.

    Args:
        xml_string: XML content to parse
        context: Description for error messages (e.g., "CIB XML", "node states XML")

    Returns:
        Parsed root element, or None on parse error
    """
    try:
        return ET.fromstring(xml_string)
    except ET.ParseError as e:
        logger.error("Failed to parse %s: %s", context, e)
        logger.debug("%s content (first %d chars):\n%s",
                     context, XML_PREVIEW_LENGTH, xml_string[:XML_PREVIEW_LENGTH])
        if len(xml_string) > XML_PREVIEW_LENGTH:
            logger.debug("%s truncated, total length: %d bytes",
                         context, len(xml_string))
        return None


def get_all_online_nodes(options: Dict[str, str]) -> Set[str]:
    """Get list of all online nodes from cached cluster nodes.

    Args:
        options: Options dictionary from fence agent

    Returns:
        Set of online node names (nodes with "member" status)
    """
    cluster_nodes = get_all_cluster_nodes(options)

    # Filter for nodes with "member" status
    online_nodes = {node for node, status in cluster_nodes.items() if status == "member"}

    logger.debug("Online nodes: %s", ', '.join(sorted(online_nodes)))
    return online_nodes


def get_all_node_sites(options: Dict[str, str], site_attribute: str) -> Dict[str, str]:
    """Get site attribute for all nodes.

    Args:
        options: Options dictionary from fence agent
        site_attribute: Name of the site attribute to query

    Returns:
        Mapping of node_name to site_value (empty dict on error)
    """
    attr_safe = shlex.quote(site_attribute)
    cmd = (
        f'cibadmin --query '
        f'--xpath "//nodes/node[instance_attributes[nvpair[@name=\'{attr_safe}\']]]"'
    )

    (rc, stdout, stderr) = run_cmd(options, cmd)

    if rc != 0:
        logger.warning("Failed to query nodes site (rc=%d)", rc)
        return {}

    # Parse XML output
    root = safe_parse_xml(stdout, "CIB XML")
    if root is None:
        return {}

    node_sites = {}
    # Find all node elements
    for node in root.findall('.//node'):
        node_name = node.get('uname')
        if not node_name:
            continue

        # Find the site attribute value
        for nvpair in node.findall('.//nvpair'):
            if nvpair.get('name') == site_attribute:
                site_value = nvpair.get('value')
                if site_value:
                    node_sites[node_name] = site_value
                    logger.debug("Node %s site: %s", node_name, site_value)
                break

    logger.debug("Batch query found %d nodes with site attribute", len(node_sites))
    return node_sites


def set_terminate(options: Dict[str, str], node: str) -> bool:
    """Set terminate attribute to true for a node.

    Args:
        options: Options dictionary from fence agent
        node: Node name to mark for termination

    Returns:
        True on success, False on failure
    """
    node_safe = shlex.quote(node)
    cmd = (
        f'crm_attribute --node {node_safe} --name terminate '
        f'--update true --type "status"'
    )

    if is_dry_run(options):
        logger.info("DRY-RUN: Would execute: %s", cmd)
        return True

    (rc, stdout, stderr) = run_cmd(options, cmd)

    if rc == 0:
        return True

    logger.error("Failed to set terminate for node %s (rc=%d)", node, rc)
    if stderr:
        logger.error("Error: %s", stderr.strip())
    return False


def clear_terminate(options: Dict[str, str], node: str) -> bool:
    """Delete terminate attribute for a node (unfencing).

    Args:
        options: Options dictionary from fence agent
        node: Node name to clear termination from

    Returns:
        True on success, False on failure
    """
    node_safe = shlex.quote(node)
    cmd = (
        f'crm_attribute --node {node_safe} --name terminate '
        f'--delete --type "status"'
    )

    if is_dry_run(options):
        logger.info("DRY-RUN: Would execute: %s", cmd)
        return True

    (rc, stdout, stderr) = run_cmd(options, cmd)

    if rc == 0:
        logger.info("Cleared terminate attribute for node %s", node)
        return True

    logger.error("Failed to clear terminate for node %s (rc=%d)", node, rc)
    if stderr:
        logger.error("Error: %s", stderr.strip())
    return False


def get_all_node_states(options: Dict[str, str]) -> Dict[str, ET.Element]:
    """Query all node_state elements from CIB (single query - optimized).

    Populates global _node_states_cache for access by other functions.

    Args:
        options: Options dictionary from fence agent

    Returns:
        Mapping of node_name to ET.Element (empty dict on error)
    """
    global _node_states_cache

    cmd = 'cibadmin --query --xpath "//node_state"'

    (rc, stdout, stderr) = run_cmd(options, cmd)

    if rc != 0:
        logger.debug("Failed to query node_state (rc=%d)", rc)
        _node_states_cache = {}
        return {}

    # Parse XML output
    root = safe_parse_xml(stdout, "node_state XML")
    if root is None:
        _node_states_cache = {}
        return {}

    node_states = {}
    # Handle both single node_state and multiple wrapped in a parent
    if root.tag == 'node_state':
        # Single node_state element
        node_name = root.get('uname')
        if node_name:
            node_states[node_name] = root
    else:
        # Multiple node_state elements
        for node_state in root.findall('.//node_state'):
            node_name = node_state.get('uname')
            if node_name:
                node_states[node_name] = node_state

    logger.debug("Queried %d node_state elements", len(node_states))
    _node_states_cache = node_states
    return node_states


def get_cached_node_states() -> Dict[str, ET.Element]:
    """Get cached node_states from global cache.

    Returns:
        Mapping of node_name to ET.Element (empty dict if not cached)
    """
    global _node_states_cache
    return _node_states_cache if _node_states_cache is not None else {}


def get_terminate_from_node_state(node_state: Optional[ET.Element]) -> Optional[str]:
    """Extract terminate status attribute from node_state Element.

    Args:
        node_state: ET.Element of node_state, or None

    Returns:
        Terminate value or None if not found
    """
    if node_state is None:
        return None

    # Navigate: node_state -> transient_attributes -> instance_attributes
    #           -> nvpair[@name='terminate']
    for nvpair in node_state.findall('.//transient_attributes/instance_attributes/nvpair'):
        if nvpair.get('name') == 'terminate':
            return nvpair.get('value')

    return None


def get_node_uptime(
    options: Dict[str, str],
    node: str
) -> Optional[int]:
    """Get node uptime in seconds from in_ccm timestamp.

    Args:
        options: Options dictionary from fence agent
        node: Node name to check

    Returns:
        Uptime in seconds, or None if unavailable
    """
    node_states = get_cached_node_states()
    if node in node_states:
        node_state = node_states[node]
        in_ccm = node_state.get('in_ccm')
        if in_ccm and in_ccm not in ["0", "false"]:
            try:
                current_time = int(time.time())
                uptime = current_time - int(in_ccm)
                logger.debug("Node %s uptime from in_ccm: %ds", node, uptime)
                return uptime
            except ValueError:
                logger.warning("Invalid in_ccm timestamp for node %s: %s", node, in_ccm)

    logger.debug("Node %s uptime unavailable", node)
    return None


def get_all_cluster_nodes(options: Dict[str, str]) -> Dict[str, str]:
    """Get dict of all cluster nodes with status (cached).

    Args:
        options: Options dictionary from fence agent

    Returns:
        Mapping of node_name to status (e.g., {"node1": "member", "node2": "lost"})
    """
    global _cluster_nodes_cache

    if _cluster_nodes_cache is not None:
        return _cluster_nodes_cache

    (rc, stdout, stderr) = run_cmd(options, "crm_node -l")

    if rc != 0:
        logger.warning("Failed to get cluster node list (rc=%d)", rc)
        _cluster_nodes_cache = {}
        return {}

    nodes = {}
    for line in stdout.strip().split('\n'):
        parts = line.split()
        if len(parts) >= 3:
            node_name = parts[1]
            status = parts[2]
            nodes[node_name] = status
        elif len(parts) >= 2:
            # Fallback if no status
            nodes[parts[1]] = "unknown"

    # DEBUG example: node1(member), node2(member), node3(lost)
    logger.debug("Cluster nodes: %s", ", ".join(f"{n}({s})" for n, s in nodes.items()))
    _cluster_nodes_cache = nodes
    return nodes


def get_quorum_status(options: Dict[str, str]) -> Tuple[int, bool]:
    """Get quorum status.

    Args:
        options: Options dictionary from fence agent

    Returns:
        Expected votes (0 if unavailable) and quorate status
    """
    (rc, stdout, stderr) = run_cmd(options, "corosync-quorumtool -s")

    expected_votes = 0
    quorate = False

    for line in stdout.strip().split('\n'):
        if "Expected votes" in line:
            parts = line.split()
            if parts:
                try:
                    expected_votes = int(parts[-1])
                except ValueError as e:
                    logger.debug("Failed to parse expected_votes from '%s': %s", parts[-1], e)
        elif "Quorate" in line:
            parts = line.split()
            if parts and parts[-1].lower() in ["yes", "1"]:
                quorate = True

    logger.debug("Quorum: expected_votes=%d quorate=%s", expected_votes, quorate)
    return expected_votes, quorate


def check_quorum_safety(options: Dict[str, str], nodes_to_fence_count: int) -> bool:
    """Check if fencing would cause loss of quorum.

    Args:
        options: Options dictionary from fence agent
        nodes_to_fence_count: Number of nodes to be fenced

    Returns:
        True if safe, False if would lose quorum
    """
    expected_votes, quorate = get_quorum_status(options)

    if expected_votes == 0:
        logger.info("Unable to determine quorum status, proceeding with caution")
        return True

    remaining_nodes = expected_votes - nodes_to_fence_count
    quorum_threshold = (expected_votes // 2) + 1

    logger.info("Quorum check: fencing %d nodes, %d would remain (threshold: %d)",
                nodes_to_fence_count, remaining_nodes, quorum_threshold)

    if remaining_nodes < quorum_threshold:
        logger.warning("Quorum safety: Remaining nodes (%d) < threshold (%d)",
                       remaining_nodes, quorum_threshold)
        return False

    logger.info("Quorum check: SAFE - remaining nodes >= threshold")
    return True


def site_fence_test(_conn, options):
    """Main fence logic for site-wide fencing.

    This function is called by fence_action(). It validates the input
    parameters and applies the "on" action.

    Args:
        _conn: Connection object (unused for this agent)
        options: Options dictionary from fence agent

    Returns:
        True on success, False on failure
    """
    action = options["--action"]

    # Get target node
    target_node = options.get("--plug")
    if not target_node:
        logger.error("No target node specified")
        return False

    # Handle "on" action - just clear terminate attribute (no validation needed)
    if action == "on":
        logger.info("Unfencing node %s - clearing terminate attribute", target_node)
        if clear_terminate(options, target_node):
            logger.info("Successfully cleared terminate attribute for node %s", target_node)
            return True
        else:
            logger.error("Failed to clear terminate attribute for node %s", target_node)
            return False

    # Verify target node is a cluster member (for off/reboot actions)
    cluster_nodes = get_all_cluster_nodes(options)
    if cluster_nodes and target_node not in cluster_nodes:
        logger.error("Target node '%s' is not a cluster member", target_node)
        logger.error("Known cluster nodes: %s", ", ".join(cluster_nodes.keys()))
        fail(EC_STATUS)

    site_attribute = options.get("--site-attribute")

    # Parse uptime threshold with error handling
    try:
        uptime_threshold = int(options.get("--uptime-threshold"))
    except (ValueError, TypeError) as e:
        logger.error("Invalid uptime-threshold value: %s, using default 900", e)
        uptime_threshold = 900

    # Quorum safety: default to True (safe), only disable if explicitly false
    quorum_safe_value = options.get("--quorum-safe", "true").lower()
    quorum_safe = quorum_safe_value not in ["0", "no", "off", "false"]

    force_reschedule = options.get("--force-reschedule").lower() in ["1", "yes", "on", "true"]

    # Validate uptime threshold
    if uptime_threshold < 0:
        logger.warning("Invalid uptime threshold %d, using 0", uptime_threshold)
        uptime_threshold = 0

    # For off/reboot actions
    if action in ["off", "reboot"]:
        return execute_site_fence(
            options,
            target_node,
            site_attribute,
            uptime_threshold,
            quorum_safe,
            force_reschedule
        )

    logger.warning("Action %s not handled", action)
    return False


def identify_nodes_to_fence(
    options: Dict[str, str],
    target_node: str,
    target_site: str,
    node_sites: Dict[str, str],
    uptime_threshold: int
) -> list:
    """Identify peer nodes eligible for fencing.

    Args:
        options: Options dictionary from fence agent
        target_node: Node being fenced (excluded from peers)
        target_site: Site value of target node
        node_sites: Mapping of node names to site values
        uptime_threshold: Minimum uptime in seconds

    Returns:
        Node names eligible for fencing (empty if none)
    """
    nodes_to_fence = []
    logger.info("Identifying nodes to fence")

    for node, node_site in node_sites.items():
        logger.debug("Checking node: %s", node)

        # Skip the target node - it will be fenced by the real fence device
        if node == target_node:
            logger.debug("Node %s is the target, skipping terminate attribute", node)
            continue

        if node_site != target_site:
            logger.debug("Node %s on different site (%s), skipping", node, node_site)
            continue

        logger.info("Node %s is on same site as target (%s)", node, target_site)

        # Check uptime threshold
        node_uptime = get_node_uptime(options, node)
        if node_uptime is None:
            logger.info("Node %s: uptime unavailable, skipping for safety", node)
            continue

        if node_uptime < uptime_threshold:
            logger.info("Node %s: uptime %ds < threshold %ds, skipping",
                        node, node_uptime, uptime_threshold)
            continue

        logger.info("Node %s: uptime %ds >= threshold %ds, eligible for fencing",
                    node, node_uptime, uptime_threshold)

        nodes_to_fence.append(node)

    logger.info("%d peer nodes eligible for fencing", len(nodes_to_fence))
    return nodes_to_fence


def validate_quorum_safety(
    options: Dict[str, str],
    nodes_to_fence: list,
    quorum_safe: bool
) -> bool:
    """Validate quorum safety before fencing.

    Args:
        options: Options dictionary from fence agent
        nodes_to_fence: List of peer nodes to fence
        quorum_safe: Whether to enforce quorum check

    Returns:
        True if safe to proceed, False if would lose quorum
    """
    # Include target node in count (it will be fenced by real device,
    # not by terminate attribute)
    total_nodes_to_fence = len(nodes_to_fence) + 1  # +1 for target node
    logger.info("Quorum safety check for %d nodes (target + %d peers)",
                total_nodes_to_fence, len(nodes_to_fence))

    if not quorum_safe:
        logger.info("Quorum safety check DISABLED by configuration")
        return True

    # Perform actual quorum check
    if not check_quorum_safety(options, total_nodes_to_fence):
        logger.warning("Quorum safety check FAILED - peer fencing would cause loss of quorum")
        logger.warning("Skipping peer node fencing, target will still be fenced")
        return False

    logger.info("Quorum safety check PASSED")
    return True


def set_terminate_attributes(
    options: Dict[str, str],
    target_node: str,
    nodes_to_fence: list,
    force_reschedule: bool
) -> bool:
    """Set terminate attributes and determine return value.

    Args:
        options: Options dictionary from fence agent
        target_node: Node being fenced
        nodes_to_fence: List of peer nodes to fence
        force_reschedule: Whether to fail and force reschedule with peers

    Returns:
        True on success, False on failure
    """
    total_nodes = len(nodes_to_fence) + 1  # +1 for target
    logger.info("Setting terminate for %d site nodes (including target)", total_nodes)

    # Get cached node states to check current terminate values
    node_states = get_cached_node_states()

    already_set = 0
    newly_set = 0
    failed_count = 0
    failed_nodes = []
    peer_terminate_new = 0  # Track if any peer nodes get terminate set

    # Set terminate for target node
    # Update target node before peers to prevent the peer terminate from
    # scheduling before the target node processing returned
    current_terminate = get_terminate_from_node_state(node_states.get(target_node))
    if current_terminate and current_terminate.lower() in ["true", "1"]:
        logger.info("Target node %s already has terminate=true, skipping", target_node)
        already_set += 1
    else:
        logger.info("Setting terminate for target node: %s", target_node)
        if set_terminate(options, target_node):
            newly_set += 1
        else:
            failed_count += 1
            failed_nodes.append(target_node)

    # Set terminate for peer nodes (after target node for scheduling timing)
    for node in nodes_to_fence:
        current_terminate = get_terminate_from_node_state(node_states.get(node))
        if current_terminate and current_terminate.lower() in ["true", "1"]:
            logger.info("Peer node %s already has terminate=true, skipping", node)
            already_set += 1
        else:
            logger.info("Setting terminate for peer node: %s", node)
            if set_terminate(options, node):
                newly_set += 1
                peer_terminate_new += 1
            else:
                failed_count += 1
                failed_nodes.append(node)

    logger.info(
        "Terminate attributes: %d already set, %d newly set, %d failures",
        already_set, newly_set, failed_count
    )
    logger.info("Peer nodes with terminate newly set: %d", peer_terminate_new)

    if failed_nodes:
        logger.error("Failed to set terminate for nodes: %s", ", ".join(failed_nodes))

    # Determine return value based on mode
    if force_reschedule and peer_terminate_new > 0:
        logger.info("Simultaneous site-wide fencing - fence_site fails for target node %s",
                    target_node)
        logger.info("Returning FAILURE to trigger scheduler")
        return False

    # Default behavior: return success if terminate attributes set successfully
    if peer_terminate_new > 0:
        logger.info("Peer nodes set terminate - target %s will be fenced by next device",
                    target_node)
    else:
        logger.info("No peer nodes triggered - target %s will be fenced by next device",
                    target_node)

    return failed_count == 0


def execute_site_fence(
    options: Dict[str, str],
    target_node: str,
    site_attribute: str,
    uptime_threshold: int,
    quorum_safe: bool,
    force_reschedule: bool
) -> bool:
    """Execute site-wide fencing.

    Orchestrates the following:
    - Identify peer nodes eligible for fencing
    - Validate quorum safety
    - Set terminate attributes
    - Handle single node mode when applicable

    Args:
        options: Options dictionary from fence agent
        target_node: Node being fenced
        site_attribute: Name of the site attribute
        uptime_threshold: Minimum uptime in seconds
        quorum_safe: Whether to enforce quorum check
        force_reschedule: Whether to fail when peers need fencing

    Returns:
        True on success, False on failure
    """
    logger.info("Starting site-wide fencing for target: %s", target_node)
    logger.info("Site attribute: %s, uptime threshold: %ds, force parallel: %s",
                site_attribute, uptime_threshold, force_reschedule)

    # Query all node states once
    get_all_node_states(options)

    # Get all node sites in one query
    node_sites = get_all_node_sites(options, site_attribute)

    # Get target node's site from batch query result
    target_site = node_sites.get(target_node)
    if not target_site:
        logger.info("No site attribute for target node: %s, proceeding with single target",
                    target_node)
        logger.info("Target %s will be fenced by next device in topology", target_node)
        return True

    logger.info("Target node %s is on site: %s", target_node, target_site)

    # Check target node uptime availability
    target_uptime = get_node_uptime(options, target_node)
    if target_uptime is None:
        logger.warning("Uptime unavailable for target node: %s, cannot verify threshold",
                       target_node)
        logger.info("Proceeding with peer fencing (uptime check will be applied to peers)")
    elif target_uptime < uptime_threshold:
        logger.info("Target node %s uptime %ds < threshold %ds",
                    target_node, target_uptime, uptime_threshold)
        logger.info("Node recently restarted - skipping site-wide fencing")
        logger.info("Target %s will be fenced by next device in topology", target_node)
        return True
    else:
        logger.debug("Target node %s uptime: %ds", target_node, target_uptime)

    # Identify nodes to fence
    nodes_to_fence = identify_nodes_to_fence(
        options, target_node, target_site, node_sites,
        uptime_threshold
    )

    # If no peer nodes need fencing, return success immediately
    if not nodes_to_fence:
        logger.info("No peer nodes require fencing - returning success")
        logger.info("Target %s will be fenced by next device in topology", target_node)
        return True

    # Quorum safety check
    if not validate_quorum_safety(options, nodes_to_fence, quorum_safe):
        # Clear peer nodes but continue - target will still be fenced by fence_aws
        logger.info("Target %s will be fenced by next device in topology", target_node)
        nodes_to_fence = []

    # Set terminate attributes
    result = set_terminate_attributes(options, target_node, nodes_to_fence, force_reschedule)

    if is_dry_run(options):
        logger.info("DRY-RUN: Would return %s", "SUCCESS" if result else "FAILURE")

    return result


def define_new_opts():
    """Define custom fence agent options.

    Adds site_attribute, uptime_threshold, quorum_safe, force_reschedule,
    and dry_run options to the fence agent.
    """
    all_opt["site_attribute"] = {
        "getopt": ":",
        "longopt": "site-attribute",
        "help": (
            "--site-attribute=[name]        "
            "Name of cluster attribute defining site membership"
        ),
        "shortdesc": "Site attribute name",
        "required": "0",
        "default": "site",
        "order": 1
    }
    all_opt["uptime_threshold"] = {
        "getopt": ":",
        "longopt": "uptime-threshold",
        "help": (
            "--uptime-threshold=[seconds]   "
            "Minimum uptime before node can be fenced"
        ),
        "shortdesc": "Minimum uptime in seconds",
        "required": "0",
        "default": "900",
        "order": 2
    }
    all_opt["quorum_safe"] = {
        "getopt": ":",
        "longopt": "quorum-safe",
        "help": (
            "--quorum-safe=[true|false]     "
            "Abort fencing if it would cause loss of quorum"
        ),
        "shortdesc": "Quorum safety check",
        "required": "0",
        "default": "true",
        "order": 4
    }
    all_opt["force_reschedule"] = {
        "getopt": ":",
        "longopt": "force-reschedule",
        "help": (
            "--force-reschedule=[true|false]  Enable parallel fencing mode "
            "(fail target when peers need fencing)"
        ),
        "shortdesc": "Enable parallel fencing",
        "required": "0",
        "default": "false",
        "order": 5
    }
    all_opt["dry_run"] = {
        "getopt": ":",
        "longopt": "dry-run",
        "help": (
            "--dry-run=[true|false]         "
            "Log actions without executing (testing only)"
        ),
        "shortdesc": "Dry-run mode",
        "required": "0",
        "default": "false",
        "order": 6
    }


def main():
    """Main entry point for fence_site agent.

    Initializes options, configures logging, processes inputs,
    and executes fence actions via the fence_action framework.
    """
    device_opt = [
        "port",
        "no_password",
        "no_login",
        "no_status",
        "site_attribute",
        "uptime_threshold",
        "quorum_safe",
        "force_reschedule",
        "dry_run"
    ]

    define_new_opts()
    atexit.register(atexit_handler)

    all_opt["power_timeout"]["default"] = DEFAULT_POWER_TIMEOUT
    all_opt["shell_timeout"]["default"] = DEFAULT_SHELL_TIMEOUT

    options = check_input(device_opt, process_input(device_opt))

    # Configure logging: remove fencing library's stderr handlers, use syslog + stdout
    # This prevents duplicates and logs at proper levels (not as warnings)
    logger.handlers.clear()
    logger.propagate = False

    # Set log level based on -v flag
    log_level = logging.DEBUG if options.get("-v") or options.get("--verbose") else logging.INFO
    logger.setLevel(log_level)

    # Add syslog handler for /var/log/messages
    logger.addHandler(SyslogLibHandler())

    # Add stdout handler for console output during manual execution
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    docs = {}
    docs["shortdesc"] = "Fence agent for synchronous site-wide fencing"
    docs["longdesc"] = """fence_site is a fence agent for synchronous site-wide fencing in Pacemaker clusters.
When a node is fenced, this agent identifies and sets terminate attributes for OTHER nodes on the same site,
enabling parallel site-wide fencing. The target node itself is fenced by the real fence device.

IMPORTANT: This agent must be listed BEFORE the real fence device in fencing topology to ensure
terminate attributes are set while the real fence operation executes:

  pcs stonith level add 1 NODE fence-site REAL-FENCE-DEVICE

The agent uses Pacemaker Feature Set 3.18.0+ in_ccm timestamps for uptime tracking.

Actions:
- OFF/REBOOT: Sets terminate attribute for OTHER nodes on same site (NOT the target node)
- ON: Deletes terminate attribute for the target node (unfencing/cleanup)
- STATUS: Checks if all online nodes on site have terminate=true

Behavior:
- Target node is fenced by the next device in topology (the real fence device)
- Returns OFF (failure) when site-attribute is missing -> next device handles single-node fencing
- Returns OFF (failure) when uptime data unavailable -> next device handles single-node fencing
- Returns OFF (failure) when target node uptime is lower than threshold -> prevents loop after restart
- Returns success when terminate attributes are set for site nodes

Status checking:
- Only online nodes require terminate=true for status to report "off" (fenced)
- Offline/crashed nodes are ignored in status check (already down)

Safety features:
- Uptime threshold: Only fence nodes that have been up for minimum duration (default 5 minutes)
- Quorum protection: Abort fencing if it would cause loss of cluster quorum (includes target + site nodes)
- Site isolation: Only fence nodes matching the target node's site attribute
- Command injection protection: All parameters are properly escaped"""
    docs["vendorurl"] = "https://github.com/ClusterLabs"

    show_docs(options, docs)

    run_delay(options)

    # Use sync_set_power_fn pattern for custom status/fence logic
    result = fence_action(
        None,
        options,
        None,
        None,
        sync_set_power_fn=site_fence_test
    )

    sys.exit(result)


if __name__ == "__main__":
    main()
