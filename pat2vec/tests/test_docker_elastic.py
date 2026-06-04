import unittest
from unittest.mock import patch, MagicMock, mock_open
import subprocess
import requests
from pat2vec.util.docker_elastic import ElasticContainer


class TestElasticContainer(unittest.TestCase):

    def setUp(self):
        self.container = ElasticContainer()

    @patch("subprocess.run")
    def test_cleanup_orphans_success(self, mock_run):
        # Mock finding two containers and removing them
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="cid1\ncid2\n"),
            MagicMock(returncode=0),
        ]
        ElasticContainer.cleanup_orphans()
        self.assertEqual(mock_run.call_count, 2)
        mock_run.assert_any_call(
            ["docker", "rm", "-f", "cid1", "cid2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    @patch("subprocess.run")
    def test_cleanup_orphans_error(self, mock_run):
        # Mock failure in listing
        mock_run.return_value = MagicMock(returncode=1, stderr="some error")
        with self.assertLogs("pat2vec.util.docker_elastic", level="WARNING") as cm:
            ElasticContainer.cleanup_orphans()
            self.assertTrue(
                any("Could not list orphaned containers" in line for line in cm.output)
            )

    @patch("subprocess.run")
    def test_ensure_image_already_present(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        self.assertTrue(self.container._ensure_image())
        mock_run.assert_called_with(
            ["docker", "image", "inspect", self.container.image],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    @patch("subprocess.run")
    @patch("time.sleep")
    def test_ensure_image_pull_retry(self, mock_sleep, mock_run):
        # inspect fails, pull fails twice then succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1),  # inspect
            MagicMock(returncode=1, stderr="fail"),  # pull 1
            MagicMock(returncode=1, stderr="fail"),  # pull 2
            MagicMock(returncode=0),  # pull 3
        ]
        self.assertTrue(self.container._ensure_image())
        self.assertEqual(mock_run.call_count, 4)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("socket.socket")
    def test_is_port_free(self, mock_socket):
        mock_conn = MagicMock()
        mock_socket.return_value.__enter__.return_value = mock_conn

        # Case: Port is free
        mock_conn.connect_ex.return_value = 1
        self.assertTrue(self.container._is_port_free(1234))

        # Case: Port is busy
        mock_conn.connect_ex.return_value = 0
        self.assertFalse(self.container._is_port_free(1234))

    @patch("subprocess.run")
    def test_get_mapped_port(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="0.0.0.0:32768\n")
        self.assertEqual(self.container._get_mapped_port(), 32768)

        mock_run.return_value = MagicMock(returncode=1)
        self.assertEqual(self.container._get_mapped_port(), 19200)  # returns default

    @patch("subprocess.run")
    def test_get_container_ip(self, mock_run):
        self.container.container_id = "test_cid"
        mock_run.return_value = MagicMock(returncode=0, stdout="172.17.0.5\n")
        self.assertEqual(self.container._get_container_ip(), "172.17.0.5")

    @patch("os.path.exists")
    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="Iface\tDestination\tGateway\neth0\t00000000\t010011AC\n",
    )
    def test_get_host_gateway_ip_proc(self, mock_file, mock_exists):
        mock_exists.return_value = True
        # 010011AC is 172.17.0.1 (little endian hex)
        self.assertEqual(self.container._get_host_gateway_ip(), "172.17.0.1")

    @patch("os.path.exists", return_value=False)
    @patch("subprocess.run")
    def test_get_host_gateway_ip_cmd(self, mock_run, mock_exists):
        mock_run.return_value = MagicMock(
            returncode=0, stdout="default via 192.168.1.1 dev eth0\n"
        )
        self.assertEqual(self.container._get_host_gateway_ip(), "192.168.1.1")

    @patch.object(ElasticContainer, "cleanup_orphans")
    @patch("subprocess.run")
    def test_start_docker_missing(self, mock_run, mock_cleanup):
        mock_run.side_effect = FileNotFoundError()
        self.assertFalse(self.container.start())

    @patch.object(ElasticContainer, "cleanup_orphans")
    @patch.object(ElasticContainer, "_ensure_image", return_value=True)
    @patch.object(ElasticContainer, "_is_port_free", return_value=True)
    @patch.object(ElasticContainer, "_get_mapped_port", return_value=19200)
    @patch.object(ElasticContainer, "_wait_for_ready")
    @patch("subprocess.run")
    @patch("atexit.register")
    def test_start_success(
        self,
        mock_atexit,
        mock_run,
        mock_wait,
        mock_port,
        mock_free,
        mock_image,
        mock_cleanup,
    ):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # version check
            MagicMock(returncode=0, stdout="new_id\n"),  # docker run
        ]
        self.assertTrue(self.container.start())
        self.assertEqual(self.container.container_id, "new_id")
        mock_atexit.assert_called_with(self.container.stop)

    @patch("subprocess.run")
    def test_stop(self, mock_run):
        self.container.container_id = "test_id"
        self.container.stop()
        mock_run.assert_called_with(
            ["docker", "rm", "-f", "test_id"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertIsNone(self.container.container_id)

    @patch("requests.Session")
    def test_wait_for_ready_success_first_try(self, mock_session):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "green"}
        mock_session.return_value.get.return_value = mock_resp

        with (
            patch.object(
                self.container, "_get_container_ip", return_value="172.17.0.2"
            ),
            patch.object(
                self.container, "_get_host_gateway_ip", return_value="172.17.0.1"
            ),
        ):

            self.container._wait_for_ready(timeout=5)
            self.assertEqual(self.container.host, "127.0.0.1")

    @patch("requests.Session")
    @patch("time.sleep")
    @patch("time.time")
    def test_wait_for_ready_failure_then_success(
        self, mock_time, mock_sleep, mock_session
    ):
        mock_time.side_effect = range(0, 100, 1)

        fail_resp = requests.exceptions.RequestException()
        success_resp = MagicMock(status_code=200)
        success_resp.json.return_value = {"status": "yellow"}

        # Targets: 127.0.0.1, gateway, host.docker.internal, container_ip
        # Fail the first loop iteration (4 targets), succeed on the 5th call
        mock_session.return_value.get.side_effect = [
            fail_resp,
            fail_resp,
            fail_resp,
            fail_resp,
            success_resp,
        ]

        with patch.object(
            self.container, "_get_container_ip", return_value="172.17.0.2"
        ):
            self.container._wait_for_ready(timeout=20)
            self.assertEqual(self.container.host, "127.0.0.1")
            self.assertEqual(mock_session.return_value.get.call_count, 5)

    @patch("requests.Session")
    @patch("time.time", return_value=100)
    def test_wait_for_ready_timeout(self, mock_time, mock_session):
        # Advance time immediately to trigger timeout
        mock_time.side_effect = [100, 200]
        mock_session.return_value.get.side_effect = (
            requests.exceptions.RequestException()
        )

        with self.assertRaises(TimeoutError):
            self.container._wait_for_ready(timeout=10)

    @patch.object(ElasticContainer, "start", return_value=True)
    @patch.object(ElasticContainer, "stop")
    def test_context_manager(self, mock_stop, mock_start):
        with ElasticContainer() as ec:
            self.assertIsInstance(ec, ElasticContainer)
            mock_start.assert_called_once()
        mock_stop.assert_called_once()

    def test_get_credentials(self):
        self.container.host = "localhost"
        self.container.port = 9200
        url, user, pwd = self.container.get_credentials()
        self.assertEqual(url, "http://localhost:9200")
        self.assertEqual(user, "elastic")
        self.assertEqual(pwd, "test_password_123")


if __name__ == "__main__":
    unittest.main()
