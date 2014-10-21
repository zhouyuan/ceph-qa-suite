
"""
Exercise the MDS's behaviour when clients and the MDCache reach or
exceed the limits of how many caps/inodes they should hold.
"""

import contextlib
import json
import logging
import time
from unittest import SkipTest
import errno

from teuthology.orchestra.run import CommandFailedError

from tasks.cephfs.filesystem import Filesystem
from tasks.cephfs.fuse_mount import FuseMount
from tasks.cephfs.cephfs_test_case import CephFSTestCase, run_tests


log = logging.getLogger(__name__)


# Arbitrary timeouts for operations involving restarting
# an MDS or waiting for it to come up
MDS_RESTART_GRACE = 60

# Hardcoded values from Server::recall_client_state
CAP_RECALL_RATIO = 0.8
CAP_RECALL_MIN = 100


def wait_until_equal(get_fn, expect_val, timeout, reject_fn=None):
    period = 5
    elapsed = 0
    while True:
        val = get_fn()
        if val == expect_val:
            return
        elif reject_fn and reject_fn(val):
            raise RuntimeError("wait_until_equal: forbidden value {0} seen".format(val))
        else:
            if elapsed >= timeout:
                raise RuntimeError("Timed out after {0} seconds waiting for {1} (currently {2})".format(
                    elapsed, expect_val, val
                ))
            else:
                log.debug("wait_until_equal: {0} != {1}, waiting...".format(val, expect_val))
            time.sleep(period)
            elapsed += period

    log.debug("wait_until_equal: success")


def wait_until_true(condition, timeout):
    period = 5
    elapsed = 0
    while True:
        if condition():
            return
        else:
            if elapsed >= timeout:
                raise RuntimeError("Timed out after {0} seconds".format(elapsed))
            else:
                log.debug("wait_until_equal: waiting...")
            time.sleep(period)
            elapsed += period

    log.debug("wait_until_equal: success")


class TestClusterFull(CephFSTestCase):
    # Environment references
    mount_a = None
    mount_b = None

    def setUp(self):

        if self.mount_a.is_mounted():
            self.mount_a.umount_wait()

        if self.mount_b.is_mounted():
            self.mount_b.umount_wait()

        self.fs.mds_restart()
        self.fs.wait_for_daemons()
        if not self.mount_a.is_mounted():
            self.mount_a.mount()
            self.mount_a.wait_until_mounted()

        if not self.mount_b.is_mounted():
            self.mount_b.mount()
            self.mount_b.wait_until_mounted()

        self.mount_a.run_shell(["rm", "-rf", "*"])

    def tearDown(self):
        self.fs.clear_firewall()
        self.mount_a.teardown()
        self.mount_b.teardown()

    def test_barrier(self):
        """
        That when an OSD epoch barrier is set on an MDS, subsequently
        issued capabilities cause clients to update their OSD map to that
        epoch.
        """

        # Check the initial barrier epoch on the MDS: this should be
        # set to the latest map at MDS startup
        # TODO

        initial_client_epoch = self.mount_a.get_osd_epoch()[0]
        self.assertEqual(initial_client_epoch, self.mount_b.get_osd_epoch()[0])

        # Set and unset a flag to cause OSD epoch to increment
        self.fs.mon_manager.raw_cluster_cmd("osd", "set", "pause")
        self.fs.mon_manager.raw_cluster_cmd("osd", "unset", "pause")

        out = self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json").strip()
        new_epoch = json.loads(out)['epoch']
        self.assertNotEqual(initial_client_epoch, new_epoch)

        # Do a metadata operation on client A, witness that it ends up with
        # the old OSD map from startup time (nothing has prompted it
        # to update its map)

        self.mount_b.open_no_data("alpha")
        mount_a_epoch, mount_a_barrier = self.mount_a.get_osd_epoch()
        self.assertEqual(mount_a_epoch, initial_client_epoch)
        #self.assertEqual(mount_a_barrier, initial_epoch)  # TODO

        # Set a barrier on the MDS
        self.fs.mds_asok(["osdmap", "barrier", new_epoch.__str__()])

        # Do an operation on client B, witness that it ends up with
        # the latest OSD map from the barrier
        self.mount_b.run_shell(["touch", "bravo"])
        self.mount_b.open_no_data("bravo")
        mount_b_epoch, mount_b_barrier = self.mount_b.get_osd_epoch()

        self.assertEqual(mount_b_epoch, new_epoch)
        self.assertEqual(mount_b_barrier, new_epoch)

    def _test_full(self, easy_case):
        """
        - That a client trying to write data to a file is prevented
        from doing so with an -EFULL result
        - That they are also prevented from creating new files by the MDS.
        - That they may delete another file to get the system healthy again

        :param easy_case: if true, delete a successfully written file to
                          free up space.  else, delete the file that experienced
                          the failed write.
        """

        osd_mon_report_interval_max = int(self.fs.get_config("osd_mon_report_interval_max"))
        mon_osd_full_ratio = float(self.fs.get_config("mon_osd_full_ratio"))

        data_pool_names = self.fs.get_data_pool_names()
        if len(data_pool_names) > 1:
            raise RuntimeError("This test can't handle multiple data pools")
        else:
            data_pool_name = data_pool_names[0]

        pool_capacity = self.fs.get_pool_df(data_pool_name)['max_avail']
        fill_mb = int(1.05 * mon_osd_full_ratio * (pool_capacity / (1024.0 * 1024.0))) + 2

        log.info("Writing {0}MB should fill this cluster".format(fill_mb))

        # Fill up the cluster
        self.mount_a.write_n_mb("large_file_a", fill_mb / 2)
        self.mount_a.write_n_mb("large_file_b", fill_mb / 2)

        # large_file_b at this stage has some dirty data that can't be
        # flushed to the full cluster, so

        wait_until_true(lambda: 'full' in json.loads(
            self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['flags'],
            timeout=osd_mon_report_interval_max * 1.5
        )

        # Attempting to write more data should give me ENOSPC
        with self.assertRaises(CommandFailedError) as ar:
            self.mount_a.write_n_mb("large_file_b", 50, seek=fill_mb / 2)
        #self.assertEqual(ar.exception.exitstatus, errno.ENOSPC)
        self.assertEqual(ar.exception.exitstatus, 1)  # dd returns 1 on "No space"

        # Wait for the MDS to see the latest OSD map so that it will reliably
        # be applying the free space policy
        osd_epoch = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['epoch']
        wait_until_true(
            lambda: self.fs.mds_asok(['status'])['osdmap_epoch'] >= osd_epoch,
            timeout=10)

        with self.assertRaises(CommandFailedError):
            # A kink in the MDS behaviour: it doesn't see the new OSD map
            # until it journals the first file creation, so it's only
            # the second file creation that hits the ENOSPC condition on
            # the MDS side
            self.mount_a.write_n_mb("small_file_1", 0)

        # Clear out some space
        if easy_case:
            self.mount_a.run_shell(['rm', '-f', 'large_file_a'])
            self.mount_a.run_shell(['rm', '-f', 'large_file_b'])
        else:
            self.mount_a.run_shell(['rm', '-f', 'large_file_b'])
            self.mount_a.run_shell(['rm', '-f', 'large_file_a'])

        wait_until_true(lambda: 'full' not in json.loads(
            self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['flags'],
            timeout=osd_mon_report_interval_max * 1.5
        )

        # Wait for the MDS to see the latest OSD map so that it will reliably
        # be applying the free space policy
        osd_epoch = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['epoch']
        wait_until_true(
            lambda: self.fs.mds_asok(['status'])['osdmap_epoch'] >= osd_epoch,
            timeout=10)

        # Now I should be able to write again
        self.mount_a.write_n_mb("large_file", 50, seek=0)

        # Ensure that the MDS keeps its OSD epoch barrier across a restart

    def test_full_different_file(self):
        self._test_full(True)

    def test_full_same_file(self):
        self._test_full(False)

@contextlib.contextmanager
def task(ctx, config):
    fs = Filesystem(ctx, config)

    # Pick out the clients we will use from the configuration
    # =======================================================
    if len(ctx.mounts) < 2:
        raise RuntimeError("Need at least two clients")
    mount_a = ctx.mounts.values()[0]
    mount_b = ctx.mounts.values()[1]

    # Stash references on ctx so that we can easily debug in interactive mode
    # =======================================================================
    ctx.filesystem = fs
    ctx.mount_a = mount_a
    ctx.mount_b = mount_b

    run_tests(ctx, config, TestClusterFull, {
        'fs': fs,
        'mount_a': mount_a,
        'mount_b': mount_b
    })

    # Continue to any downstream tasks
    # ================================
    yield
