#!/usr/bin/python3 -tt

# Fence agent for synchronous site-wide fencing in Pacemaker clusters
#
# When a node is fenced, this agent identifies and fences all other nodes
# with the same site attribute, enabling parallel site-wide fencing.

import re
import shlex
import sys
import logging
import atexit
import time
sys.path.append("/usr/share/fence")
from fencing import fail, fail_usage, run_command, fence_action, all_opt
from fencing import atexit_handler, check_input, process_input, show_docs
from fencing import run_delay, EC_GENERIC_ERROR

def get_node_online_status(options, node):
	"""Check if node is online in cluster using crm_mon

	Returns:
		bool: True if node is online, False otherwise
	"""
	(rc, stdout, stderr) = run_command(options, "crm_mon -1")

	if rc != 0:
		logging.warning("crm_mon failed (rc=%d)", rc)
		return False

	# Parse: "Online: [ node1 node2 node3 ]"
	online_match = re.search(r'Online:\s*\[(.*?)\]', stdout.strip(), re.MULTILINE)
	if online_match:
		online_nodes = online_match.group(1).split()
		is_online = node in online_nodes
		logging.debug("Node %s is %s", node, "ONLINE" if is_online else "OFFLINE")
		return is_online

	logging.debug("No online nodes found in crm_mon output")
	return False

def get_cluster_attribute(options, node, attribute):
	"""Get cluster attribute for a node

	Returns:
		str or None: Attribute value, or None if not found
	"""
	node_safe = shlex.quote(node)
	attr_safe = shlex.quote(attribute)
	cmd = f'crm_attribute --node {node_safe} --query --name {attr_safe} --quiet 2>/dev/null'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0 and stdout:
		value = stdout.strip()
		if value and value != "(null)":
			logging.debug("Node %s attribute %s: %s", node, attribute, value)
			return value

	logging.debug("Node %s has no attribute %s", node, attribute)
	return None

def get_status_attribute(options, node, attribute):
	"""Get status attribute for a node

	Returns:
		str or None: Attribute value, or None if not found
	"""
	node_safe = shlex.quote(node)
	attr_safe = shlex.quote(attribute)
	cmd = f'crm_attribute --node {node_safe} --query --name {attr_safe} --type "status" --quiet 2>/dev/null'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0 and stdout:
		value = stdout.strip()
		if value:
			logging.debug("Node %s status attribute %s: %s", node, attribute, value)
			return value

	return None

def set_status_attribute(options, node, attribute, value):
	"""Set status attribute for a node

	Returns:
		bool: True on success, False on failure
	"""
	node_safe = shlex.quote(node)
	attr_safe = shlex.quote(attribute)
	value_safe = shlex.quote(value)
	cmd = f'crm_attribute --node {node_safe} --name {attr_safe} --update {value_safe} --type "status"'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0:
		logging.info("Set %s=%s for node %s", attribute, value, node)
		return True

	logging.error("Failed to set %s for node %s (rc=%d)", attribute, node, rc)
	if stderr:
		logging.error("Error: %s", stderr.strip())
	return False

def delete_status_attribute(options, node, attribute):
	"""Delete status attribute for a node

	Returns:
		bool: True on success, False on failure
	"""
	node_safe = shlex.quote(node)
	attr_safe = shlex.quote(attribute)
	cmd = f'crm_attribute --node {node_safe} --name {attr_safe} --delete --type "status"'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0:
		logging.info("Deleted %s for node %s", attribute, node)
		return True

	# rc=6 means attribute doesn't exist - that's ok
	if rc == 6:
		logging.debug("Attribute %s does not exist for node %s (already deleted)", attribute, node)
		return True

	logging.error("Failed to delete %s for node %s (rc=%d)", attribute, node, rc)
	if stderr:
		logging.error("Error: %s", stderr.strip())
	return False

def check_feature_set(options):
	"""Check if Pacemaker Feature Set 3.18.0+ is available (supports in_ccm)"""
	cmd = 'crm_attribute --query --type status --name "#feature-set" --quiet 2>/dev/null'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0 and stdout:
		version = stdout.strip()
		if version and version != "(null)":
			try:
				parts = version.split('.')
				major = int(parts[0])
				minor = int(parts[1]) if len(parts) > 1 else 0

				if major > 3 or (major == 3 and minor >= 18):
					logging.debug("Feature Set %s supports in_ccm", version)
					return True
			except (ValueError, IndexError) as e:
				logging.debug("Feature set parse failed: %s", e)

	logging.debug("Feature Set does not support in_ccm, using fallback")
	return False

def get_in_ccm_timestamp(options, node):
	"""Get in_ccm timestamp from CIB for Pacemaker 3.18.0+"""
	node_safe = shlex.quote(node)
	cmd = f'cibadmin --query --xpath "//node_state[@uname={node_safe}]"' # 2>/dev/null'

	(rc, stdout, stderr) = run_command(options, cmd)

	if rc == 0:
		match = re.search(r'in_ccm="([^"]*)"', stdout.strip())
		if match and match.group(1) not in ["0", "false"]:
			return match.group(1)

	return None

def get_node_uptime(options, node, join_attribute, supports_in_ccm):
	"""Get node uptime in seconds

	Returns:
		int or None: Uptime in seconds, or None if unavailable
	"""
	# Try Feature Set 3.18.0+ in_ccm first
	if supports_in_ccm:
		in_ccm = get_in_ccm_timestamp(options, node)
		if in_ccm:
			try:
				current_time = int(time.time())
				uptime = current_time - int(in_ccm)
				logging.debug("Node %s uptime from in_ccm: %ds", node, uptime)
				return uptime
			except ValueError:
				logging.warning("Invalid in_ccm timestamp for node %s: %s", node, in_ccm)

	# Fallback to join_attribute
	value = get_cluster_attribute(options, node, join_attribute)
	if value:
		try:
			join_time = int(value)
			current_time = int(time.time())
			uptime = current_time - join_time
			logging.debug("Node %s uptime from %s: %ds", node, join_attribute, uptime)
			return uptime
		except ValueError:
			logging.warning("Invalid join timestamp for node %s: %s", node, value)

	logging.debug("Node %s uptime unavailable", node)
	return None

def get_all_cluster_nodes(options):
	"""Get list of all cluster nodes"""
	(rc, stdout, stderr) = run_command(options, "crm_node -l")

	if rc != 0:
		logging.warning("Failed to get cluster node list (rc=%d)", rc)
		return []

	nodes = []
	for line in stdout.strip().split('\n'):
		parts = line.split()
		if len(parts) >= 2:
			nodes.append(parts[1])

	return nodes

def get_quorum_status(options):
	"""Get quorum status

	Returns:
		tuple: (expected_votes, quorate_bool)
	"""
	(rc, stdout, stderr) = run_command(options, "corosync-quorumtool -s")

	expected_votes = 0
	quorate = False

	for line in stdout.strip().split('\n'):
		if "Expected votes" in line:
			parts = line.split()
			if parts:
				try:
					expected_votes = int(parts[-1])
				except ValueError:
					pass
		elif "Quorate" in line:
			parts = line.split()
			if parts and parts[-1].lower() in ["yes", "1"]:
				quorate = True

	logging.debug("Quorum: expected_votes=%d quorate=%s", expected_votes, quorate)
	return expected_votes, quorate

def check_quorum_safety(options, nodes_to_fence_count):
	"""Check if fencing would cause loss of quorum

	Returns:
		bool: True if safe, False if would lose quorum
	"""
	expected_votes, quorate = get_quorum_status(options)

	if expected_votes == 0:
		logging.info("Unable to determine quorum status, proceeding with caution")
		return True

	remaining_nodes = expected_votes - nodes_to_fence_count
	quorum_threshold = (expected_votes // 2) + 1

	logging.info("Quorum check: fencing %d nodes, %d would remain (threshold: %d)",
		nodes_to_fence_count, remaining_nodes, quorum_threshold)

	if remaining_nodes < quorum_threshold:
		logging.error("SAFETY: Fencing would cause loss of quorum!")
		logging.error("SAFETY: Remaining nodes (%d) < threshold (%d)", remaining_nodes, quorum_threshold)
		return False

	logging.info("Quorum check: SAFE - remaining nodes >= threshold")
	return True

def site_fence_test(conn, options):
	"""Main fence logic for site-wide fencing

	This function is called by fence_action() for both status checks
	and fence operations.
	"""
	action = options["--action"]

	# Get parameters
	target_node = options.get("--plug")
	if not target_node:
		logging.error("No target node specified")
		return False

	# Handle "on" action - clear terminate attribute
	if action == "on":
		logging.info("Unfencing node %s - clearing terminate attribute", target_node)
		if delete_status_attribute(options, target_node, "terminate"):
			logging.info("Successfully cleared terminate attribute for node %s", target_node)
			return True
		else:
			logging.error("Failed to clear terminate attribute for node %s", target_node)
			return False

	site_attribute = options.get("--site-attribute", "site")
	uptime_threshold = int(options.get("--uptime-threshold", "300"))
	join_attribute = options.get("--join-attribute", "node_join_time")
	quorum_safe = options.get("--quorum-safe", "true").lower() in ["1", "yes", "on", "true"]

	# Validate uptime threshold
	if uptime_threshold < 0:
		logging.warning("Invalid uptime threshold %d, using 0", uptime_threshold)
		uptime_threshold = 0

	# For status check
	if action == "status":
		return get_site_status(options, target_node, site_attribute)

	# For off/reboot actions
	if action in ["off", "reboot"]:
		return execute_site_fence(
			options,
			target_node,
			site_attribute,
			uptime_threshold,
			join_attribute,
			quorum_safe
		)

	logging.warning("Action %s not handled", action)
	return False

def get_site_status(options, target_node, site_attribute):
	"""Check if site is fenced (for status action)

	Returns:
		bool: True if site is "on" (not fenced), False if "off" (fenced)
	"""
	logging.debug("Status check for node %s", target_node)

	# Get target node's site
	target_site = get_cluster_attribute(options, target_node, site_attribute)
	if not target_site:
		logging.debug("Node %s has no site attribute, checking only target", target_node)
		# Fallback: check only target node
		terminate = get_status_attribute(options, target_node, "terminate")
		if terminate and terminate.lower() in ["true", "1"]:
			logging.debug("Node %s has terminate=true, returning off", target_node)
			return False  # off = fenced
		logging.debug("Node %s has no terminate, returning on", target_node)
		return True  # on = not fenced

	# Get all nodes on the same site
	all_nodes = get_all_cluster_nodes(options)
	site_nodes = [n for n in all_nodes if get_cluster_attribute(options, n, site_attribute) == target_site]

	if not site_nodes:
		logging.debug("No nodes found on site %s, returning on", target_site)
		return True  # on = not fenced

	logging.debug("Checking status for %d nodes on site %s", len(site_nodes), target_site)

	# Check status of all nodes on site
	online_nodes_total = 0
	online_nodes_terminated = 0
	offline_nodes = 0

	for node in site_nodes:
		if get_node_online_status(options, node):
			online_nodes_total += 1
			terminate = get_status_attribute(options, node, "terminate")
			if terminate and terminate.lower() in ["true", "1"]:
				online_nodes_terminated += 1
				logging.debug("Node %s (ONLINE) has terminate=true", node)
			else:
				logging.debug("Node %s (ONLINE) does NOT have terminate=true", node)
		else:
			offline_nodes += 1
			logging.debug("Node %s is OFFLINE, skipping terminate check", node)

	logging.debug("Site %s status: %d/%d online nodes terminated, %d offline",
		target_site, online_nodes_terminated, online_nodes_total, offline_nodes)

	# Return False (off/fenced) if all online nodes are terminated
	if online_nodes_total > 0 and online_nodes_terminated == online_nodes_total:
		logging.debug("All online nodes terminated, returning off")
		return False  # off = fenced

	if online_nodes_total == 0 and offline_nodes > 0:
		# All nodes offline - site is down
		logging.debug("All nodes offline, returning off")
		return False  # off = fenced

	logging.debug("Not all online nodes terminated, returning on")
	return True  # on = not fenced

def execute_site_fence(options, target_node, site_attribute, uptime_threshold, join_attribute, quorum_safe):
	"""Execute site-wide fencing

	Returns:
		bool: True on success, False on failure
	"""
	logging.info("Starting site-wide fencing for target: %s", target_node)
	logging.info("Site attribute: %s, uptime threshold: %ds", site_attribute, uptime_threshold)

	# Cache feature set check
	supports_in_ccm = check_feature_set(options)

	# Get target node's site
	target_site = get_cluster_attribute(options, target_node, site_attribute)
	if not target_site:
		logging.info("No site attribute for target node: %s, returning OFF", target_node)
		logging.info("Single-node fencing will be handled by next device in topology")
		return False

	logging.info("Target node %s is on site: %s", target_node, target_site)

	# Check target node uptime availability
	target_uptime = get_node_uptime(options, target_node, join_attribute, supports_in_ccm)
	if target_uptime is None:
		logging.info("Uptime unavailable for target node: %s, returning OFF", target_node)
		logging.info("Single-node fencing will be handled by next device in topology")
		return False

	logging.debug("Target node %s uptime: %ds", target_node, target_uptime)

	# Check if target node uptime is below threshold
	if target_uptime < uptime_threshold:
		logging.info("Target node %s uptime %ds < threshold %ds, returning OFF",
			target_node, target_uptime, uptime_threshold)
		logging.info("Node recently restarted - skipping site-wide fencing")
		logging.info("Single-node fencing will be handled by next device in topology")
		return False

	# Phase 1: Identify nodes to fence
	all_nodes = get_all_cluster_nodes(options)
	if not all_nodes:
		logging.error("Failed to retrieve cluster node list")
		return False

	nodes_to_fence = []
	logging.info("Phase 1: Identifying nodes to fence")

	for node in all_nodes:
		if not node:
			continue

		logging.debug("Checking node: %s", node)

		# Skip the target node - it will be fenced by the real fence device
		if node == target_node:
			logging.debug("Node %s is the target, skipping terminate attribute", node)
			continue

		node_site = get_cluster_attribute(options, node, site_attribute)
		if node_site != target_site:
			logging.debug("Node %s on different site (%s), skipping", node, node_site)
			continue

		logging.info("Node %s is on same site as target (%s)", node, target_site)

		# Check uptime threshold
		node_uptime = get_node_uptime(options, node, join_attribute, supports_in_ccm)
		if node_uptime is None:
			logging.info("Node %s: uptime unavailable, skipping for safety", node)
			continue

		if node_uptime < uptime_threshold:
			logging.info("Node %s: uptime %ds < threshold %ds, skipping",
				node, node_uptime, uptime_threshold)
			continue

		logging.info("Node %s: uptime %ds >= threshold %ds, eligible for fencing",
			node, node_uptime, uptime_threshold)

		nodes_to_fence.append(node)

	# Phase 2: Quorum safety check
	# Include target node in count (it will be fenced by real device, not by terminate attribute)
	total_nodes_to_fence = len(nodes_to_fence) + 1  # +1 for target node
	logging.info("Phase 2: Quorum safety check for %d nodes (%d with terminate + target)",
		total_nodes_to_fence, len(nodes_to_fence))

	if len(nodes_to_fence) == 0:
		logging.info("No other nodes on site require terminate attribute")
		logging.info("Only target node will be fenced by real device, allowing operation")
		# Continue - still safe to fence just the target node
	elif quorum_safe:
		if not check_quorum_safety(options, total_nodes_to_fence):
			logging.error("Quorum safety check FAILED - aborting")
			return False
	else:
		logging.info("Quorum safety check DISABLED by configuration")

	# Phase 3: Set terminate attributes (excluding target node)
	logging.info("Phase 3: Setting terminate for %d site nodes (excluding target)", len(nodes_to_fence))

	if len(nodes_to_fence) == 0:
		logging.info("No other nodes on site - only target will be fenced by real device")
		return True

	fenced_count = 0
	failed_count = 0
	failed_nodes = []

	for node in nodes_to_fence:
		logging.info("Setting terminate for node: %s", node)
		if set_status_attribute(options, node, "terminate", "true"):
			fenced_count += 1
		else:
			failed_count += 1
			failed_nodes.append(node)

	logging.info("Terminate attributes set: %d succeeded, %d failures", fenced_count, failed_count)
	logging.info("Target node %s will be fenced by real device", target_node)

	if failed_nodes:
		logging.error("Failed to set terminate for nodes: %s", ", ".join(failed_nodes))

	# Return True if we set terminate successfully (or no other nodes to set)
	return failed_count == 0

def define_new_opts():
	all_opt["site_attribute"] = {
		"getopt": ":",
		"longopt": "site-attribute",
		"help": "--site-attribute=[name]        Name of cluster attribute defining site membership",
		"shortdesc": "Site attribute name",
		"required": "0",
		"default": "site",
		"order": 1
	}
	all_opt["uptime_threshold"] = {
		"getopt": ":",
		"longopt": "uptime-threshold",
		"help": "--uptime-threshold=[seconds]   Minimum uptime before node can be fenced",
		"shortdesc": "Minimum uptime in seconds",
		"required": "0",
		"default": "300",
		"order": 2
	}
	all_opt["join_attribute"] = {
		"getopt": ":",
		"longopt": "join-attribute",
		"help": "--join-attribute=[name]        Attribute storing node join timestamp",
		"shortdesc": "Join attribute name",
		"required": "0",
		"default": "node_join_time",
		"order": 3
	}
	all_opt["quorum_safe"] = {
		"getopt": ":",
		"longopt": "quorum-safe",
		"help": "--quorum-safe=[true|false]     Abort fencing if it would cause loss of quorum",
		"shortdesc": "Quorum safety check",
		"required": "0",
		"default": "true",
		"order": 4
	}

def main():
	device_opt = [
		"port",
		"no_password",
		"no_login",
		"no_status",
		"site_attribute",
		"uptime_threshold",
		"join_attribute",
		"quorum_safe"
	]

	define_new_opts()
	atexit.register(atexit_handler)

	all_opt["power_timeout"]["default"] = "60"
	all_opt["shell_timeout"]["default"] = "30"

	options = check_input(device_opt, process_input(device_opt))

	docs = {}
	docs["shortdesc"] = "Fence agent for synchronous site-wide fencing"
	docs["longdesc"] = """fence_site is a fence agent for synchronous site-wide fencing in Pacemaker clusters.
When a node is fenced, this agent identifies and sets terminate attributes for OTHER nodes on the same site,
enabling parallel site-wide fencing. The target node itself is fenced by the real fence device.

IMPORTANT: This agent must be listed BEFORE the real fence device in fencing topology to ensure
terminate attributes are set while the real fence operation executes:

  pcs stonith level add 1 &lt;node&gt; fence-site,&lt;real-fence-device&gt;

The agent uses Pacemaker Feature Set 3.18.0+ in_ccm timestamps when available, falling back
to a custom join_attribute for older versions. Use alert-uptime-helper to maintain join times
on older Pacemaker installations.

Actions:
- OFF/REBOOT: Sets terminate attribute for OTHER nodes on same site (NOT the target node)
- ON: Deletes terminate attribute for the target node (unfencing/cleanup)
- STATUS: Checks if all online nodes on site have terminate=true

Behavior:
- Target node is fenced by the next device in topology (the real fence device)
- Returns OFF (failure) when site-attribute is missing → next device handles single-node fencing
- Returns OFF (failure) when uptime data unavailable → next device handles single-node fencing
- Returns OFF (failure) when target node uptime < threshold → prevents loop after restart
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
