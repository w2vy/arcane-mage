#!/usr/bin/env python3
"""
Standalone provisioning script for arcane-mage.

Usage:
    python provision.py <node-config.yaml>

Reads hypervisor credentials from ~/.fluxnode_creator.yaml and provisions
the node(s) defined in the given config file.
"""

from __future__ import annotations

import asyncio
import gzip
import importlib.resources as resources
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Literal

from pyfatfs.PyFatFS import PyFatFS

import yaml

from arcane_mage.models import (
    ArcaneCreatorConfig,
    ArcaneOsConfig,
    ArcaneOsConfigGroup,
    HypervisorConfig,
)
from arcane_mage.proxmox import ProxmoxApi

# Image resources from the package
_images_ref = resources.files("arcane_mage.images")
EFI_GZ_RESOURCE = _images_ref / "arcane_efi.raw.gz"
CONFIG_GZ_RESOURCE = _images_ref / "arcane_config.raw.gz"
CONFIG_IMAGE_BASE = "arcane_config"
CONFIG_FILE_PATH = Path("/arcane_config.yaml")

TIER_MAP = {
    "cumulus": {"memory_mb": "8192", "scsi_gb": "220", "cpu_cores": 4},
    "nimbus": {"memory_mb": "32768", "scsi_gb": "440", "cpu_cores": 8},
    "stratus": {"memory_mb": "65536", "scsi_gb": "880", "cpu_cores": 16},
}

MIN_VERSION = [8, 4, 1]


def step(msg: str) -> None:
    print(f"  -> {msg}")


def ok(msg: str) -> None:
    print(f"  [OK] {msg}")


def fail(msg: str) -> None:
    print(f"  [FAIL] {msg}")


def is_api_min_version(version: str) -> bool:
    parts = version.split(".")
    if len(parts) != 3:
        return False
    for actual_str, required in zip(parts, MIN_VERSION):
        try:
            actual = int(actual_str)
        except ValueError:
            return False
        if actual > required:
            return True
        elif actual < required:
            return False
    return True


def get_vm_config_file_name(vm_id: int) -> str:
    return f"{vm_id}_{CONFIG_IMAGE_BASE}.raw"


async def find_hypervisor_for_node(
    node_name: str, hypervisors: list[HypervisorConfig]
) -> tuple[ProxmoxApi, HypervisorConfig] | None:
    """Try each hypervisor to find which one serves the given node."""
    for hv_config in hypervisors:
        credential = hv_config.real_credential()
        if not credential:
            print(f"  Skipping {hv_config.url} - no credential")
            continue

        api = None
        if hv_config.auth_type == "token":
            token = ProxmoxApi.parse_token(credential)
            if token:
                api = ProxmoxApi.from_token(hv_config.url, *token)
        elif hv_config.auth_type == "userpass":
            user_pass = ProxmoxApi.parse_user_pass(credential)
            if user_pass:
                api = await ProxmoxApi.from_user_pass(hv_config.url, *user_pass)

        if not api:
            print(f"  Skipping {hv_config.url} - could not create API client")
            continue

        step(f"Checking {hv_config.url} for node '{node_name}'...")
        nodes_res = await api.get_hypervisor_nodes()

        if not nodes_res:
            msg = "Timeout" if nodes_res.timed_out else nodes_res.error
            print(f"  Skipping {hv_config.url} - {msg}")
            await api.close()
            continue

        for node in nodes_res.payload:
            if node.get("node") == node_name:
                ok(f"Found node '{node_name}' on {hv_config.url}")
                return api, hv_config

        await api.close()

    return None


async def validate_api_version(api: ProxmoxApi, node: str) -> bool:
    step("Validating API version...")
    res = await api.get_api_version(node)
    if not res:
        fail("Unable to get Proxmox API version")
        return False
    version = res.payload.get("version")
    if not version:
        fail("API payload missing version info")
        return False
    if not is_api_min_version(version):
        fail(f"API version too old. Got: {version}, Want: 8.4.1")
        return False
    ok(f"API version: {version}")
    return True


async def validate_storage(
    api: ProxmoxApi,
    node: str,
    storage_iso: str,
    storage_images: str,
    storage_import: str,
) -> bool:
    step(f"Validating storage (iso={storage_iso}, images={storage_images}, import={storage_import})...")
    res = await api.get_storage_state(node)
    if not res:
        fail("Unable to get Proxmox storage state")
        return False
    if not res.payload:
        fail("No storage state available, did you forget API permissions?")
        return False

    node_storage_iso = next(
        filter(lambda x: x.get("storage") == storage_iso, res.payload), None
    )
    node_storage_images = next(
        filter(lambda x: x.get("storage") == storage_images, res.payload), None
    )
    node_storage_import = next(
        filter(lambda x: x.get("storage") == storage_import, res.payload), None
    )

    if not all([node_storage_iso, node_storage_images, node_storage_import]):
        missing = []
        if not node_storage_iso:
            missing.append(f"iso ({storage_iso})")
        if not node_storage_images:
            missing.append(f"images ({storage_images})")
        if not node_storage_import:
            missing.append(f"import ({storage_import})")
        fail(f"Missing storage: {', '.join(missing)}")
        return False

    iso_content = node_storage_iso.get("content", "")
    images_content = node_storage_images.get("content", "")
    import_content = node_storage_import.get("content", "")

    if "iso" not in iso_content:
        fail(f"Storage '{storage_iso}' does not have 'iso' content type")
        return False
    if "images" not in images_content:
        fail(f"Storage '{storage_images}' does not have 'images' content type")
        return False
    if "import" not in import_content:
        fail(f"Storage '{storage_import}' does not have 'import' content type")
        return False

    import_available = node_storage_import.get("avail", 0)
    if import_available < 10485760:
        fail(f"Import storage space < 10MiB ({import_available} bytes available)")
        return False

    ok("Storage validated")
    return True


async def validate_iso(
    api: ProxmoxApi, node: str, iso_name: str, storage_iso: str
) -> bool:
    step(f"Validating ISO '{iso_name}' on {storage_iso}...")
    res = await api.get_storage_content(node, storage_iso)
    if not res:
        fail("Unable to get storage content")
        return False

    iso_exists = next(
        filter(
            lambda x: x.get("content") == "iso"
            and x.get("volid", "").endswith(iso_name),
            res.payload,
        ),
        None,
    )

    if not iso_exists:
        fail(f"ISO '{iso_name}' not found on {storage_iso}")
        return False

    ok("ISO found")
    return True


async def validate_network(api: ProxmoxApi, node: str, network: str) -> bool:
    step(f"Validating network '{network}'...")
    res = await api.get_networks(node)
    if not res:
        fail("Unable to get networks")
        return False

    network_exists = next(
        filter(lambda x: x.get("iface") == network, res.payload), None
    )

    if not network_exists:
        fail(f"Network '{network}' not found on hypervisor")
        return False

    ok("Network validated")
    return True


async def upload_efi(api: ProxmoxApi, node: str, storage: str) -> bool:
    step("Uploading EFI image...")
    with EFI_GZ_RESOURCE.open("rb") as f:
        efi_disk = gzip.decompress(f.read())

    upload_res = await api.upload_file(
        efi_disk, node=node, storage=storage, file_name="arcane_efi.raw"
    )

    if not upload_res:
        fail(f"EFI upload failed: {upload_res.error}")
        return False

    task_ok = await api.wait_for_task(upload_res.payload, node)
    if not task_ok:
        fail("EFI upload task did not complete")
        return False

    ok("EFI image uploaded")
    return True


async def upload_config(
    api: ProxmoxApi, config_bytes: bytes, vm_id: int, node: str, storage: str
) -> bool:
    step("Uploading config image...")
    with tempfile.TemporaryDirectory(prefix="arcane_mage_") as tmpdir:
        config_image_name = get_vm_config_file_name(vm_id)
        config_image_path = Path(tmpdir) / config_image_name

        with config_image_path.open("wb") as img_fh:
            with CONFIG_GZ_RESOURCE.open("rb") as img_gz_fh:
                img_fh.write(gzip.decompress(img_gz_fh.read()))

        fat_fs = PyFatFS(filename=str(config_image_path))
        with fat_fs.open(str(CONFIG_FILE_PATH), mode="wb") as conf_fh:
            conf_fh.write(config_bytes)

        upload_res = await api.upload_file(config_image_path, node=node, storage=storage)

    if not upload_res:
        fail(f"Config upload failed: {upload_res.error}")
        return False

    task_ok = await api.wait_for_task(upload_res.payload, node)
    if not task_ok:
        fail("Config upload task did not complete")
        return False

    ok("Config image uploaded")
    return True


async def create_vm_config(
    api: ProxmoxApi,
    vm_name: str,
    tier: Literal["cumulus", "nimbus", "stratus"],
    network_bridge: str,
    storage_images: str,
    storage_iso: str,
    storage_import: str,
    iso_name: str,
    vm_id: int | None = None,
    startup_config: str | None = None,
    disk_limit: int | None = None,
    cpu_limit: float | None = None,
    network_limit: int | None = None,
) -> dict | None:
    tier_config = TIER_MAP.get(tier)
    if not tier_config:
        fail(f"Unknown tier: {tier}")
        return None

    if vm_id is None:
        step("Getting next VM ID...")
        vm_id_res = await api.get_next_id()
        if not vm_id_res:
            fail("Unable to get next VM ID")
            return None
        vm_id = vm_id_res.payload
        ok(f"VM ID: {vm_id}")

    disk_rate = f"mbps_rd={disk_limit},mbps_wr={disk_limit}," if disk_limit else ""
    network_rate = f",rate={network_limit}" if network_limit else ""
    cpu_limit = cpu_limit or 0

    smbios_uuid = str(uuid.uuid4())
    config_img = get_vm_config_file_name(vm_id)

    config = {
        "efidisk0": f"{storage_images}:0,efitype=4m,pre-enrolled-keys=0,import-from={storage_import}:import/arcane_efi.raw",
        "cpu": "host",
        "ostype": "l26",
        "sockets": 1,
        "vmid": vm_id,
        "agent": "1",
        "onboot": 1,
        "name": vm_name,
        "smbios1": f"uuid={smbios_uuid}",
        "boot": "order=scsi0;ide2;net0",
        "numa": 0,
        "memory": tier_config["memory_mb"],
        "tpmstate0": f"{storage_images}:4,version=v2.0",
        "cores": tier_config["cpu_cores"],
        "cpulimit": cpu_limit,
        "bios": "ovmf",
        "scsi0": f"{storage_images}:{tier_config['scsi_gb']},{disk_rate}discard=on,iothread=1,ssd=1",
        "scsi1": f"{storage_images}:0,import-from={storage_import}:import/{config_img}",
        "ide2": f"{storage_iso}:iso/{iso_name},media=cdrom",
        "net0": f"model=virtio,bridge={network_bridge}{network_rate}",
        "scsihw": "virtio-scsi-single",
    }

    if startup_config:
        config["startup"] = startup_config

    return config


async def create_vm(api: ProxmoxApi, config: dict, node: str) -> bool:
    step("Creating VM...")
    create_res = await api.create_vm(config, node)
    if not create_res:
        fail(f"VM creation failed: {create_res.error}")
        return False

    task_ok = await api.wait_for_task(create_res.payload, node)
    if not task_ok:
        fail("VM creation task did not complete")
        return False

    ok("VM created")
    return True


async def delete_install_disks(
    api: ProxmoxApi, vm_id: int, node: str, storage: str, delete_efi: bool = True
) -> bool:
    step("Cleaning up install disks...")
    config_file = f"{vm_id}_arcane_config.raw"

    if delete_efi:
        efi_res = await api.delete_file("arcane_efi.raw", node, storage, content="import")
    else:
        efi_res = True

    config_res = await api.delete_file(config_file, node, storage, content="import")

    if not efi_res or not config_res:
        fail("Unable to delete install disks")
        return False

    if delete_efi:
        efi_ok = await api.wait_for_task(efi_res.payload, node)
        if not efi_ok:
            fail("EFI disk cleanup task did not complete")
            return False

    config_ok = await api.wait_for_task(config_res.payload, node)
    if not config_ok:
        fail("Config disk cleanup task did not complete")
        return False

    ok("Install disks cleaned up")
    return True


async def start_vm(api: ProxmoxApi, vm_id: int, node: str) -> bool:
    step(f"Starting VM {vm_id}...")
    res = await api.start_vm(vm_id, node)
    if not res:
        fail(f"Unable to start VM: {res.error}")
        return False

    task_ok = await api.wait_for_task(res.payload, node, 20)
    if not task_ok:
        fail("VM start task did not complete")
        return False

    ok("VM started")
    return True


async def provision_node(
    api: ProxmoxApi, fluxnode: ArcaneOsConfig, delete_efi: bool = True
) -> bool:
    hv = fluxnode.hypervisor

    if not hv:
        fail("No hypervisor config in node definition")
        return False

    if hv.node_tier not in TIER_MAP:
        fail(f"Unknown tier: {hv.node_tier}")
        return False

    # Validate
    if not await validate_api_version(api, hv.node):
        return False

    if not await validate_storage(api, hv.node, hv.storage_iso, hv.storage_images, hv.storage_import):
        return False

    if not await validate_iso(api, hv.node, hv.iso_name, hv.storage_iso):
        return False

    if not await validate_network(api, hv.node, hv.network):
        return False

    # Build VM config
    step("Generating VM config...")
    vm_config = await create_vm_config(
        api,
        vm_name=hv.vm_name,
        tier=hv.node_tier,
        network_bridge=hv.network,
        storage_images=hv.storage_images,
        storage_iso=hv.storage_iso,
        storage_import=hv.storage_import,
        iso_name=hv.iso_name,
        vm_id=hv.vm_id,
        startup_config=hv.startup_config,
        disk_limit=hv.disk_limit,
        cpu_limit=hv.cpu_limit,
        network_limit=hv.network_limit,
    )

    if not vm_config:
        fail("Unable to generate VM config")
        return False

    vm_id: int = vm_config["vmid"]
    ok(f"VM config generated (vmid={vm_id})")

    # Upload config image
    config_upload = yaml.dump({"nodes": [fluxnode.to_dict()]})
    if not await upload_config(api, config_upload.encode("utf-8"), vm_id, hv.node, hv.storage_import):
        return False

    # Upload EFI image
    if not await upload_efi(api, hv.node, hv.storage_import):
        return False

    # Create VM
    if not await create_vm(api, vm_config, node=hv.node):
        await delete_install_disks(api, vm_id, hv.node, hv.storage_import, delete_efi)
        return False

    # Clean up install disks
    if not await delete_install_disks(api, vm_id, hv.node, hv.storage_import, delete_efi):
        return False

    # Start VM
    if hv.start_on_creation:
        if not await start_vm(api, vm_id, hv.node):
            return False

    return True


async def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <node-config.yaml>")
        return 1

    config_path = Path(sys.argv[1])
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        return 1

    # Load node config
    print(f"Loading node config: {config_path}")
    with open(config_path) as f:
        raw = yaml.safe_load(f.read())

    try:
        node_group = ArcaneOsConfigGroup.from_dict(raw)
    except Exception as e:
        print(f"Failed to parse node config: {e}")
        return 1

    print(f"Found {len(node_group.nodes)} node(s) to provision")

    # Load hypervisor credentials
    print(f"Loading hypervisor config: {ArcaneCreatorConfig.config_path}")
    creator_config = ArcaneCreatorConfig.from_fs()

    if not creator_config.hypervisors:
        print("No hypervisors configured in ~/.fluxnode_creator.yaml")
        return 1

    print(f"Found {len(creator_config.hypervisors)} hypervisor(s)")

    results: list[tuple[str, bool]] = []

    for i, fluxnode in enumerate(node_group.nodes):
        hv = fluxnode.hypervisor
        if not hv:
            print(f"\n[Node {i+1}] No hypervisor config - skipping")
            results.append(("unknown", False))
            continue

        label = f"{hv.node}:{hv.vm_name}"
        print(f"\n{'='*60}")
        print(f"[Node {i+1}] Provisioning {label} ({hv.node_tier})")
        print(f"{'='*60}")

        # Find which hypervisor serves this node
        result = await find_hypervisor_for_node(hv.node, creator_config.hypervisors)
        if not result:
            fail(f"No hypervisor found that serves node '{hv.node}'")
            results.append((label, False))
            continue

        api, hv_config = result

        try:
            success = await provision_node(api, fluxnode)
            results.append((label, success))
        except Exception as e:
            fail(f"Exception during provisioning: {e}")
            import traceback
            traceback.print_exc()
            results.append((label, False))
        finally:
            await api.close()

    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    for label, success in results:
        status = "OK" if success else "FAILED"
        print(f"  {label}: {status}")

    return 0 if all(s for _, s in results) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
