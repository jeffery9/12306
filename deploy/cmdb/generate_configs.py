import os
import json

def load_cmdb():
    cmdb_path = os.path.join(os.path.dirname(__file__), 'inventory.json')
    with open(cmdb_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def render_template(template_content, cmdb):
    mapping = {
        "{{ cluster_name }}": cmdb["cluster"]["name"],
        "{{ cluster_token }}": cmdb["cluster"]["etcd_token"],
        
        # Ports
        "{{ haproxy_stats }}": cmdb["ports"]["haproxy_stats"],
        "{{ haproxy_write }}": cmdb["ports"]["haproxy_write"],
        "{{ haproxy_read }}": cmdb["ports"]["haproxy_read"],
        "{{ pgbouncer_listen }}": cmdb["ports"]["pgbouncer_listen"],
        "{{ postgres_port }}": cmdb["ports"]["postgres_port"],
        "{{ patroni_rest_port }}": cmdb["ports"]["patroni_rest_port"],
        "{{ exporter_port }}": cmdb["ports"]["exporter_port"],
        
        # Hosts
        "{{ haproxy_host }}": cmdb["hosts"]["haproxy"],
        "{{ pgbouncer_host }}": cmdb["hosts"]["pgbouncer"],

        # Beijing
        "{{ beijing_id }}": cmdb["regions"]["beijing"]["id"],
        "{{ beijing_name }}": cmdb["regions"]["beijing"]["name"],
        "{{ beijing_ip }}": cmdb["regions"]["beijing"]["etcd_ip"],
        "{{ beijing_peer_port }}": cmdb["regions"]["beijing"]["etcd_peer_port"],
        "{{ beijing_client_port }}": cmdb["regions"]["beijing"]["etcd_client_port"],
        "{{ beijing_pg_master }}": cmdb["regions"]["beijing"]["pg_master_name"],
        "{{ beijing_shards }}": cmdb["regions"]["beijing"]["shards"],
        
        # Shanghai
        "{{ shanghai_id }}": cmdb["regions"]["shanghai"]["id"],
        "{{ shanghai_name }}": cmdb["regions"]["shanghai"]["name"],
        "{{ shanghai_ip }}": cmdb["regions"]["shanghai"]["etcd_ip"],
        "{{ shanghai_peer_port }}": cmdb["regions"]["shanghai"]["etcd_peer_port"],
        "{{ shanghai_client_port }}": cmdb["regions"]["shanghai"]["etcd_client_port"],
        "{{ shanghai_pg_master }}": cmdb["regions"]["shanghai"]["pg_master_name"],
        "{{ shanghai_shards }}": cmdb["regions"]["shanghai"]["shards"],
        
        # Wuhan
        "{{ wuhan_id }}": cmdb["regions"]["wuhan"]["id"],
        "{{ wuhan_name }}": cmdb["regions"]["wuhan"]["name"],
        "{{ wuhan_ip }}": cmdb["regions"]["wuhan"]["etcd_ip"],
        "{{ wuhan_peer_port }}": cmdb["regions"]["wuhan"]["etcd_peer_port"],
        "{{ wuhan_client_port }}": cmdb["regions"]["wuhan"]["etcd_client_port"],
        "{{ wuhan_heartbeat }}": str(cmdb["regions"]["wuhan"]["heartbeat_interval_ms"]),
        "{{ wuhan_election }}": str(cmdb["regions"]["wuhan"]["election_timeout_ms"]),
        "{{ wuhan_quota }}": str(cmdb["regions"]["wuhan"]["quota_backend_bytes"]),

        # K8s Configs
        "{{ k8s_namespace }}": cmdb["k8s"]["namespace"],
        "{{ k8s_web_replicas }}": str(cmdb["k8s"]["web_replicas"]),
        "{{ k8s_web_image }}": cmdb["k8s"]["web_image"],
        "{{ k8s_cpu_request }}": cmdb["k8s"]["cpu_request"],
        "{{ k8s_cpu_limit }}": cmdb["k8s"]["cpu_limit"],
        "{{ k8s_memory_request }}": cmdb["k8s"]["memory_request"],
        "{{ k8s_memory_limit }}": cmdb["k8s"]["memory_limit"],
        "{{ k8s_database_url }}": cmdb["k8s"]["database_url"],
        "{{ k8s_redis_url }}": cmdb["k8s"]["redis_url"],
        "{{ k8s_kafka_bootstrap_servers }}": cmdb["k8s"]["kafka_bootstrap_servers"],
        "{{ k8s_hpa_min_replicas }}": str(cmdb["k8s"]["hpa_min_replicas"]),
        "{{ k8s_hpa_max_replicas }}": str(cmdb["k8s"]["hpa_max_replicas"]),
        "{{ k8s_hpa_cpu_utilization }}": str(cmdb["k8s"]["hpa_cpu_utilization"]),
        "{{ k8s_hpa_memory_utilization }}": str(cmdb["k8s"]["hpa_memory_utilization"]),
        "{{ k8s_keda_min_replicas }}": str(cmdb["k8s"]["keda_min_replicas"]),
        "{{ k8s_keda_max_replicas }}": str(cmdb["k8s"]["keda_max_replicas"]),
        "{{ k8s_keda_lag_threshold }}": cmdb["k8s"]["keda_lag_threshold"]
    }
    
    result = template_content
    for key, value in mapping.items():
        result = result.replace(key, value)
    return result

def generate_spine_leaf_configs(cmdb):
    sl = cmdb["spine_leaf"]
    profile_name = sl["active_profile"]
    profile = sl["profiles"][profile_name]
    
    leaf_count = profile["leaf_count"]
    spine_count = profile["spine_count"]
    spine_asn = profile["spine_asn"]
    leaf_asn_start = profile["leaf_asn_start"]
    spine_loop_start = profile["spine_loopback_start"]
    
    output = []
    output.append(f"! 🌌 12306 Spine-Leaf Fabric Auto-Generated Configurations ({profile_name.upper()} Profile)")
    output.append(f"! ⚠️ THIS FILE IS AUTO-GENERATED FROM CMDB INVENTORY. DO NOT EDIT DIRECTLY.")
    output.append("!\n")
    
    # Generate Spines
    for m in range(1, spine_count + 1):
        spine_id = spine_loop_start + m - 1
        spine_loopback0 = f"10.255.255.{spine_id}"
        output.append(f"! ────────────────────────────────────────────────────────")
        output.append(f"! Configuration for Spine-{m}")
        output.append(f"! ────────────────────────────────────────────────────────")
        output.append(f"hostname Spine-{m}")
        output.append("ip routing")
        output.append("interface Loopback0")
        output.append("   description Router-ID Underlay Core")
        output.append(f"   ip address {spine_loopback0}/32")
        output.append("!")
        
        # Ports to Leafs
        for n in range(1, leaf_count + 1):
            s = (n - 1) * spine_count * 2 + (m - 1) * 2
            octet3 = s // 256
            octet4 = s % 256
            spine_ip = f"10.0.{octet3}.{octet4}"
            output.append(f"interface Ethernet{n}")
            output.append(f"   description Link to Leaf-{n}")
            output.append("   no switchport")
            output.append(f"   ip address {spine_ip}/31")
            output.append("!")
            
        output.append(f"router bgp {spine_asn}")
        output.append(f"   router-id {spine_loopback0}")
        output.append(f"   maximum-paths {leaf_count}")
        output.append("   neighbor LEAF-PEERS peer-group")
        output.append("   neighbor LEAF-PEERS fall-over bfd")
        
        for n in range(1, leaf_count + 1):
            leaf_asn = leaf_asn_start + n - 1
            s = (n - 1) * spine_count * 2 + (m - 1) * 2
            octet3 = s // 256
            octet4 = s % 256
            leaf_ip = f"10.0.{octet3}.{octet4 + 1}"
            output.append(f"   neighbor {leaf_ip} peer-group LEAF-PEERS")
            output.append(f"   neighbor {leaf_ip} remote-as {leaf_asn}")
            
        output.append("   redistribute connected")
        output.append("!\n")
        
    # Generate Leafs
    for n in range(1, leaf_count + 1):
        leaf_asn = leaf_asn_start + n - 1
        leaf_loopback0 = f"10.255.255.{n}"
        leaf_loopback1 = f"10.250.255.{n}"
        output.append(f"! ────────────────────────────────────────────────────────")
        output.append(f"! Configuration for Leaf-{n}")
        output.append(f"! ────────────────────────────────────────────────────────")
        output.append(f"hostname Leaf-{n}")
        output.append("ip routing")
        output.append("interface Loopback0")
        output.append("   description Router-ID for eBGP")
        output.append(f"   ip address {leaf_loopback0}/32")
        output.append("!")
        output.append("interface Loopback1")
        output.append("   description VTEP Endpoint")
        output.append(f"   ip address {leaf_loopback1}/32")
        output.append("!")
        
        # Links to Spines
        for m in range(1, spine_count + 1):
            s = (n - 1) * spine_count * 2 + (m - 1) * 2
            octet3 = s // 256
            octet4 = s % 256
            leaf_ip = f"10.0.{octet3}.{octet4 + 1}"
            output.append(f"interface Ethernet{m}")
            output.append(f"   description Link to Spine-{m}")
            output.append("   no switchport")
            output.append(f"   ip address {leaf_ip}/31")
            output.append("!")
            
        output.append(f"router bgp {leaf_asn}")
        output.append(f"   router-id {leaf_loopback0}")
        output.append(f"   maximum-paths {spine_count}")
        output.append("   neighbor SPINE-PEERS peer-group")
        output.append(f"   neighbor SPINE-PEERS remote-as {spine_asn}")
        output.append("   neighbor SPINE-PEERS fall-over bfd")
        
        for m in range(1, spine_count + 1):
            s = (n - 1) * spine_count * 2 + (m - 1) * 2
            octet3 = s // 256
            octet4 = s % 256
            spine_ip = f"10.0.{octet3}.{octet4}"
            output.append(f"   neighbor {spine_ip} peer-group SPINE-PEERS")
            
        output.append("   redistribute connected route-map RM-CONN-TO-BGP")
        output.append("!")
        output.append("route-map RM-CONN-TO-BGP permit 10")
        output.append("   match interface Loopback0 Loopback1")
        output.append("!\n")
        
    return "\n".join(output)

def main():
    cmdb = load_cmdb()
    base_dir = os.path.dirname(os.path.dirname(__file__))
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    
    # Define exact target directory mappings based on template names
    target_mappings = {
        "etcd-arbitration.conf.yml.tpl": os.path.join(base_dir, "arbitration"),
        "arbitration-failover-trigger.sh.tpl": os.path.join(base_dir, "arbitration"),
        "haproxy.cfg.tpl": os.path.join(base_dir, "postgres-ha"),
        "pgbouncer.ini.tpl": os.path.join(base_dir, "postgres-ha"),
        "prometheus.yml.tpl": os.path.join(base_dir, "postgres-ha"),
        "patroni.yml.tpl": os.path.join(base_dir, "postgres-ha"),
        
        # K8s Manifests
        "k8s-deployment.yaml.tpl": os.path.join(base_dir, "k8s"),
        "k8s-service.yaml.tpl": os.path.join(base_dir, "k8s"),
        "k8s-hpa.yaml.tpl": os.path.join(base_dir, "k8s"),
        "k8s-keda.yaml.tpl": os.path.join(base_dir, "k8s")
    }

    for template_name, target_dir in target_mappings.items():
        template_path = os.path.join(templates_dir, template_name)
        if not os.path.exists(template_path):
            print(f"⚠️ Template {template_path} not found.")
            continue
            
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
            
        with open(template_path, 'r', encoding='utf-8') as f:
            template_content = f.read()
            
        rendered_content = render_template(template_content, cmdb)
        
        target_name = template_name[:-4] # remove .tpl
        target_path = os.path.join(target_dir, target_name)
        
        with open(target_path, 'w', encoding='utf-8') as f:
            f.write(rendered_content)
        
        print(f"../ Generated {target_path} from CMDB.")
        
        # Make shell scripts executable
        if target_path.endswith('.sh'):
            os.chmod(target_path, 0o755)
            
    # Auto-generate Spine-Leaf configuration fabric config from CMDB
    infra_dir = os.path.join(base_dir, "infrastructure")
    if not os.path.exists(infra_dir):
        os.makedirs(infra_dir)
        
    spine_leaf_cfg = generate_spine_leaf_configs(cmdb)
    target_sl_path = os.path.join(infra_dir, "spine_leaf_fabric_configs.cfg")
    with open(target_sl_path, 'w', encoding='utf-8') as f:
        f.write(spine_leaf_cfg)
    print(f"✅ Generated {target_sl_path} mathematically from CMDB.")

if __name__ == "__main__":
    main()