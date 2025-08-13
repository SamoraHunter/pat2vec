import os
import torch

from pat2vec.util.methods_get import get_free_gpu


def set_best_gpu(gpu_mem_threshold):
    if torch.cuda.is_available():
        gpu_index, free_mem = get_free_gpu()
    else:
        gpu_index, free_mem = -1, gpu_mem_threshold - 1  # Force CPU mode

    if int(free_mem) > gpu_mem_threshold:
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_index)
        print(f"Setting GPU {gpu_index} with {free_mem} MB free")
    else:
        os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
        print(f"Setting NO GPU, most free memory: {free_mem} MB!")
