#!/usr/bin/python3 -tt

import sys
import logging
import atexit
import subprocess
import time
from datetime import datetime

#sys.path.append("/home/jfuchs/tmp/Mar/fence/")
sys.path.append("/usr/share/fence/")
from fencing import *
from fencing import fail, fail_usage, run_delay, EC_STATUS, EC_GENERIC_ERROR, SyslogLibHandler

logger = logging.getLogger()
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(SyslogLibHandler())

def run_command(cmd, quiet=False):
	"""Execute shell command and return (rc, stdout, stderr)"""
	try:
		proc = subprocess.run(
			cmd,
			shell=True,
			capture_output=True,
			text=True,
			timeout=30
		)
		if not quiet:
			logger.debug("Command: %s", cmd)
			logger.debug("RC: %d", proc.returncode)
			if proc.stdout:
				logger.debug("STDOUT: %s", proc.stdout.strip())
			if proc.stderr:
				logger.debug("STDERR: %s", proc.stderr.strip())
		return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
	except subprocess.TimeoutExpired:
		logger.error("Command timed out: %s", cmd)
		return 1, "", "Command timed out"
	except Exception as e:
		logger.error("Command failed: %s - %s", cmd, e)
		return 1, "", str(e)

def check_feature_set():
	"""Check if Pacemaker Feature Set 3.18.0+ is available (supports in_ccm)"""
	rc, stdout, _ = run_command('crm_attribute --query --type status --name "#feature-set" --quiet 2>/dev/null', quiet=True)

	if rc == 0 and stdout and stdout != "(null)":
		try:
			parts = stdout.split('.')
			major = int(parts[0])
			minor = int(parts[1]) if len(parts) > 1 else 0

			if major > 3 or (major == 3 and minor >= 18):
				logger.debug("Feature Set %s supports in_ccm", stdout)
				return True
		except (ValueError, IndexError):
			pass

	logger.debug("Feature Set does not support in_ccm, using fallback")
	return False

def get_in_ccm_from_cib(node):
	"""Get in_ccm timestamp from CIB for Pacemaker 3.18.0+"""
	cmd = f'cibadmin --query --xpath "//node_state[@uname=\'{node}\']" 2>/dev/null | grep -oP \'in_ccm="[^"]*"\' | cut -d\'"\' -f2'
	rc, stdout, _ = run_command(cmd, quiet=True)

	if rc == 0 and stdout and stdout != "0":
		return stdout
	return None

def get_node_uptime(node, join_attribute, supports_in_ccm=None):
	"""Get node uptime in seconds

	Args:
		node: Node name
		join_attribute: Attribute name for join timestamp
		supports_in_ccm: Cached feature set check result (None = check now)
	"""
	# Use cached feature set check if provided
	if supports_in_ccm is None:
		supports_in_ccm = check_feature_set()

	# Try Feature Set 3.18.0+ in_ccm first
	if supports_in_ccm:
		in_ccm = get_in_ccm_from_cib(node)
		if in_ccm:
			try:
				current_time = int(time.time())
				uptime = current_time - int(in_ccm)
				logger.debug("Node %s uptime from in_ccm: %ds", node, uptime)
				return uptime
			except ValueError:
				pass

	# Fallback to join_attribute
	cmd = f'crm_attribute --node "{node}" --query --name "{join_attribute}" --lifetime forever --quiet 2>/dev/null'
	rc, stdout, _ = run_command(cmd, quiet=True)

	if rc == 0 and stdout and stdout != "(null)":
		try:
			join_time = int(stdout)
			current_time = int(time.time())
			uptime = current_time - join_time
			logger.debug("Node %s uptime from %s: %ds", node, join_attribute, uptime)
			return uptime
		except ValueError:
			pass

	logger.debug("Node %s uptime unavailable", node)
	return None

def get_node_site(node, site_attribute):
	"""Get site attribute for a node"""
	cmd = f'crm_attribute --node "{node}" --query --name "{site_attribute}" --quiet 2>/dev/null'
	rc, stdout, _ = run_command(cmd, quiet=True)

	if rc == 0 and stdout and stdout != "(null)":
		logger.debug("Node %s site (%s): %s", node, site_attribute, stdout)
		return stdout

	logger.debug("Node %s has no %s attribute", node, site_attribute)
	return None

def get_all_cluster_nodes():
	"""Get list of all cluster nodes"""
	rc, stdout, _ = run_command('crm_node -l', quiet=True)

	if rc != 0:
		return []

	nodes = []
	for line in stdout.split('\n'):
		parts = line.split()
		if len(parts) >= 2:
			nodes.append(parts[1])

	return nodes

def get_quorum_status():
	"""Returns (expected_votes, quorate)"""
	rc, stdout, _ = run_command('corosync-quorumtool -s 2>/dev/null', quiet=True)

	expected_votes = 0
	quorate = "No"

	for line in stdout.split('\n'):
		if "Expected votes" in line:
			parts = line.split()
			if parts:
				try:
					expected_votes = int(parts[-1])
				except ValueError:
					pass
		elif "Quorate" in line:
			parts = line.split()
			if parts:
				quorate = parts[-1]

	logger.debug("Quorum: expected_votes=%d quorate=%s", expected_votes, quorate)
	return expected_votes, quorate

def check_quorum_safety(nodes_to_fence_count):
	"""Check if fencing would cause loss of quorum"""
	expected_votes, quorate = get_quorum_status()

	if expected_votes == 0:
		logger.info("Unable to determine quorum status, proceeding with caution")
		return True

	remaining_nodes = expected_votes - nodes_to_fence_count
	quorum_threshold = (expected_votes // 2) + 1

	logger.info("Quorum check: fencing %d nodes, %d would remain (threshold: %d)",
		nodes_to_fence_count, remaining_nodes, quorum_threshold)

	if remaining_nodes < quorum_threshold:
		logger.error("SAFETY: Fencing would cause loss of quorum!")
		logger.error("SAFETY: Remaining nodes (%d) < threshold (%d)", remaining_nodes, quorum_threshold)
		return False

	logger.info("Quorum check: SAFE - remaining nodes >= threshold")
	return True

def fence_node(node):
	"""Set terminate attribute for a node"""
	logger.info("Setting terminate attribute for node: %s", node)

	cmd = f'crm_attribute --node "{node}" --name "terminate" --update "true" --type "status"'
	rc, stdout, stderr = run_command(cmd)

	if rc == 0:
		logger.info("Successfully set terminate attribute for node: %s", node)
		return True
	else:
		logger.error("Failed to set terminate attribute for node: %s (rc=%d)", node, rc)
		return False

def get_nodes_list(conn, options):
	"""List all nodes with their sites (for monitor/list actions)"""
	logger.debug("Starting list operation")
	result = {}

	site_attribute = options.get("--site-attribute", "site")
	nodes = get_all_cluster_nodes()

	for node in nodes:
		site = get_node_site(node, site_attribute)
		# Return format: {node: (name, status)}
		# We use site as "name" and "unknown" as status
		result[node] = (site or "unknown", "unknown")

	logger.debug("List operation OK: %s", result)
	return result

def get_power_status(conn, options):
	"""Get power status - check terminate attribute"""
	target_node = options.get("--plug", "unknown")
	logger.debug("Status operation for node %s", target_node)

	# Check if terminate attribute is set
	cmd = f'crm_attribute --node "{target_node}" --query --name "terminate" --type "status" --quiet 2>/dev/null'
	rc, stdout, _ = run_command(cmd, quiet=True)

    # TODO: get terminate status for all fence targets
	if rc == 0 and stdout and stdout.lower() in ["true", "1"]:
		logger.debug("Node %s has terminate=true, returning off", target_node)
		return "off"

	logger.debug("Node %s has no terminate attribute, returning on", target_node)
	return "on"

def set_power_status(conn, options):
	"""Execute site-wide fencing"""
	if options["--action"] not in ["off", "reboot"]:
		logger.info("Action '%s' not supported for fencing", options["--action"])
		return

	target_node = options.get("--plug")
	if not target_node:
		fail_usage("No target node specified (use --plug)")

	site_attribute = options.get("--site-attribute", "site")
	uptime_threshold = int(options.get("--uptime-threshold", "300"))
	join_attribute = options.get("--join-attribute", "node_join_time")
	quorum_safe = options.get("--quorum-safe", "true").lower() in ["1", "yes", "on", "true"]

	logger.info("Starting site-wide fencing for target: %s", target_node)
	logger.info("Site attribute: %s, uptime threshold: %ds", site_attribute, uptime_threshold)

	# OPTIMIZATION 2: Cache feature set check (called once instead of per-node)
	supports_in_ccm = check_feature_set()

	# Get target node's site
	target_site = get_node_site(target_node, site_attribute)

	if not target_site:
		logger.error("Cannot determine site for target node: %s", target_node)
		fail(EC_GENERIC_ERROR)

	logger.info("Target node %s is on site: %s", target_node, target_site)

	# Phase 1: Build list of nodes to fence
	nodes_to_fence = []
	all_nodes = get_all_cluster_nodes()

	logger.info("Phase 1: Identifying nodes to fence")

	for node in all_nodes:
		if not node:
			continue

		logger.debug("Checking node: %s", node)

		node_site = get_node_site(node, site_attribute)

		if node_site != target_site:
			logger.debug("Node %s on different site (%s), skipping", node, node_site)
			continue

		logger.info("Node %s is on same site as target (%s)", node, target_site)

		# Check uptime threshold - pass cached feature set check
		node_uptime = get_node_uptime(node, join_attribute, supports_in_ccm)

		if node_uptime is None:
			logger.info("Node %s: uptime unavailable, skipping for safety", node)
			continue

		if node_uptime < uptime_threshold:
			logger.info("Node %s: uptime %ds < threshold %ds, skipping",
				node, node_uptime, uptime_threshold)
			continue

		logger.info("Node %s: uptime %ds >= threshold %ds, eligible for fencing",
			node, node_uptime, uptime_threshold)

		# Include all nodes in same site (including target)
		nodes_to_fence.append(node)

	# Phase 2: Quorum safety check
	nodes_to_fence_count = len(nodes_to_fence)
	logger.info("Phase 2: Quorum safety check for %d nodes", nodes_to_fence_count)

	if nodes_to_fence_count == 0:
		logger.info("No nodes to fence (all filtered by uptime threshold)")
		return

	if quorum_safe:
		if not check_quorum_safety(nodes_to_fence_count):
			logger.error("Quorum safety check FAILED - aborting fencing operation")
			fail(EC_GENERIC_ERROR)
	else:
		logger.info("Quorum safety check DISABLED by configuration")

	# Phase 3: Execute fencing
	logger.info("Phase 3: Executing fencing for %d nodes", nodes_to_fence_count)

	fenced_count = 0
	failed_count = 0

	for node in nodes_to_fence:
		logger.info("Fencing node: %s", node)
		if fence_node(node):
			fenced_count += 1
		else:
			failed_count += 1

	logger.info("Site-wide fencing complete: %d fenced, %d failures", fenced_count, failed_count)

	# OPTIMIZATION 1: Removed redundant fence_node(target_node) call
	# Target is already included in nodes_to_fence list

	if fenced_count == 0 and failed_count > 0:
		fail(EC_GENERIC_ERROR)

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

	atexit.register(atexit_handler)

	define_new_opts()

	all_opt["power_timeout"]["default"] = "60"
	all_opt["shell_timeout"]["default"] = "30"

	options = check_input(device_opt, process_input(device_opt))

	docs = {}
	docs["shortdesc"] = "Fence agent for synchronous site-wide fencing"
	docs["longdesc"] = """fence_site is a fence agent for synchronous site-wide fencing in Pacemaker clusters.
When a node is fenced, this agent identifies and fences all other nodes with the same site attribute,
enabling parallel site-wide fencing instead of sequential per-node fencing.

IMPORTANT: This agent must be listed BEFORE the real fence device in fencing topology to ensure
terminate attributes are set while the real fence operation executes:

  pcs stonith level add 1 &lt;node&gt; fence-site,&lt;real-fence-device&gt;

The agent uses Pacemaker Feature Set 3.18.0+ in_ccm timestamps when available, falling back
to a custom join_attribute for older versions. Use alert-uptime-helper to maintain join times
on older Pacemaker installations.

Safety features:
- Uptime threshold: Only fence nodes that have been up for minimum duration (default 5 minutes)
- Quorum protection: Abort fencing if it would cause loss of cluster quorum
- Site isolation: Only fence nodes matching the target node's site attribute"""
	docs["vendorurl"] = "https://github.com/ClusterLabs"

	show_docs(options, docs)

	run_delay(options)

	# No connection needed - we use local cluster commands
	conn = None

	# Execute the fencing action
	result = fence_action(conn, options, set_power_status, get_power_status, get_nodes_list)
	sys.exit(result)

if __name__ == "__main__":
	main()
