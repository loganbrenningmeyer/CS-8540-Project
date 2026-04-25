from fabrictestbed_extensions.fablib.fablib import FablibManager

fablib = FablibManager()

slice = fablib.new_slice(name="logan-spark-cluster")

net = slice.add_l2network(name="cluster-net")

SITE = "STAR"

master = slice.add_node(
    name="master",
    site=SITE,
    cores=8,
    ram=16,
    disk=200,
    image="default_ubuntu_22"
)
master_nic = master.add_component(model="NIC_Basic", name="master_nic")
net.add_interface(master_nic.get_interfaces()[0])

for i in range(1, 4):
    worker = slice.add_node(
        name=f"worker{i}",
        site=SITE,
        cores=8,
        ram=32,
        disk=300,
        image="default_ubuntu_22"
    )
    nic = worker.add_component(model="NIC_Basic", name=f"worker{i}_nic")
    net.add_interface(nic.get_interfaces()[0])

slice.submit()
slice.wait_ssh()
slice.list_nodes()