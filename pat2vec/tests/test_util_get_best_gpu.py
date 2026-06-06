import unittest
from unittest.mock import patch
import os
from pat2vec.util.get_best_gpu import set_best_gpu


class TestGetBestGpu(unittest.TestCase):
    @patch("pat2vec.util.get_best_gpu.torch.cuda.is_available")
    @patch("pat2vec.util.get_best_gpu.get_free_gpu")
    def test_set_best_gpu_available(self, mock_get_free, mock_available):
        """Test selecting a GPU when memory is above threshold."""
        mock_available.return_value = True
        mock_get_free.return_value = (0, "8000")

        set_best_gpu(4000)
        self.assertEqual(os.environ.get("CUDA_VISIBLE_DEVICES"), "0")

    @patch("pat2vec.util.get_best_gpu.torch.cuda.is_available")
    @patch("pat2vec.util.get_best_gpu.get_free_gpu")
    def test_set_best_gpu_below_threshold(self, mock_get_free, mock_available):
        """Test forcing CPU mode when no GPU has enough memory."""
        mock_available.return_value = True
        mock_get_free.return_value = (1, "1000")

        set_best_gpu(4000)
        self.assertEqual(os.environ.get("CUDA_VISIBLE_DEVICES"), "-1")

    @patch("pat2vec.util.get_best_gpu.torch.cuda.is_available")
    def test_set_best_gpu_no_cuda(self, mock_available):
        """Test forcing CPU mode when CUDA is not present."""
        mock_available.return_value = False
        set_best_gpu(4000)
        self.assertEqual(os.environ.get("CUDA_VISIBLE_DEVICES"), "-1")
