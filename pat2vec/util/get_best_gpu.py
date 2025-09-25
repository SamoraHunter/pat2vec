import os
import torch

from pat2vec.util.methods_get import get_free_gpu

import logging

logger = logging.getLogger(__name__)

def set_best_gpu(gpu_mem_threshold: int) -> None:
    """Selects the best available GPU based on free memory.

    This function checks for available CUDA-enabled GPUs and queries their
    free memory. If a GPU is found with free memory exceeding the specified
    threshold, it sets the `CUDA_VISIBLE_DEVICES` environment variable to
    that GPU's index, effectively selecting it for subsequent processes.
    If no suitable GPU is found, it sets the variable to "-1" to force
    CPU-only mode.

    Args:
        gpu_mem_threshold: The minimum amount of free memory (in MB) required
            to select a GPU.
    """
    if torch.cuda.is_available():
        gpu_index, free_mem = get_free_gpu()
    else:
        gpu_index, free_mem = -1, gpu_mem_threshold - 1  # Force CPU mode

    if int(free_mem) > gpu_mem_threshold:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
        logger.info(f"Setting GPU {gpu_index} with {free_mem} MB free")
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
        logger.info(f"Setting NO GPU, most free memory: {free_mem} MB!")
