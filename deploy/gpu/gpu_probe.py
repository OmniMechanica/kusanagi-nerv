import csv
import ctypes
import json
import os
import socket
import subprocess

import ray


def cuda_device_count() -> int:
    cuda = ctypes.CDLL("libcuda.so.1")
    cuda.cuInit.argtypes = (ctypes.c_uint,)
    cuda.cuDeviceGetCount.argtypes = (ctypes.POINTER(ctypes.c_int),)
    status = cuda.cuInit(0)
    if status != 0:
        raise RuntimeError(f"cuInit failed with CUDA error {status}")
    count = ctypes.c_int()
    status = cuda.cuDeviceGetCount(ctypes.byref(count))
    if status != 0:
        raise RuntimeError(f"cuDeviceGetCount failed with CUDA error {status}")
    return count.value


@ray.remote(num_cpus=1, num_gpus=1, max_retries=0)
def probe() -> dict[str, object]:
    assigned = ray.get_runtime_context().get_accelerator_ids().get("GPU", [])
    visible = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    if len(assigned) != 1 or not visible or len(visible.split(",")) != 1:
        raise RuntimeError(
            f"Expected one assigned CUDA device, got {assigned=}, {visible=}"
        )
    cuda_devices = cuda_device_count()
    if cuda_devices != 1:
        raise RuntimeError(
            f"Expected one device from the CUDA driver, got {cuda_devices}"
        )

    result = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=name,memory.total,driver_version",
            "--format=csv,noheader,nounits",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    devices = list(csv.reader(result.stdout.splitlines(), skipinitialspace=True))
    if len(devices) != 1:
        raise RuntimeError(f"Expected one device from nvidia-smi, got {devices!r}")
    name, memory_mib, driver_version = devices[0]
    return {
        "worker": socket.gethostname(),
        "ray_gpu_ids": assigned,
        "cuda_visible_devices": visible,
        "cuda_device_count": cuda_devices,
        "device": name,
        "memory_mib": int(memory_mib),
        "driver_version": driver_version,
    }


def main() -> None:
    ray.init(address="auto")
    try:
        result = ray.get(probe.remote(), timeout=300)
        if result["worker"] == socket.gethostname():
            raise RuntimeError("The GPU probe unexpectedly ran on the head Pod")
        print("KUBERAY_GPU_PROBE " + json.dumps(result), flush=True)
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
