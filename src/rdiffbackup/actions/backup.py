# Copyright 2021 the rdiff-backup project
#
# This file is part of rdiff-backup.
#
# rdiff-backup is free software; you can redistribute it and/or modify
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# rdiff-backup is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with rdiff-backup; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA

"""
A built-in rdiff-backup action plug-in to backup a source to a target directory.
"""

from rdiffbackup import actions


class BackupAction(actions.BaseAction):
    """
    Backup a source directory to a target backup repository.
    """
    name = "backup"
    security = "backup"
    parent_parsers = [
        actions.CREATION_PARSER, actions.COMPRESSION_PARSER,
        actions.SELECTION_PARSER, actions.FILESYSTEM_PARSER,
        actions.USER_GROUP_PARSER, actions.STATISTICS_PARSER,
    ]

    @classmethod
    def add_action_subparser(cls, sub_handler):
        subparser = super().add_action_subparser(sub_handler)
        subparser.add_argument(
            "locations", metavar="[[USER@]SERVER::]PATH", nargs=2,
            help="locations of SOURCE_DIR and to which REPOSITORY to backup")
        return subparser

    def check(self):
        # we try to identify as many potential errors as possible before we
        # return, so we gather all potential issues and return only the final
        # result
        return_code = super().check()

        # check that the source exists and is a directory
        if not self.connected_locations[0].lstat():
            self.log("Source path {rp} does not exist".format(
                rp=self.connected_locations[0].get_safepath()), self.log.ERROR)
            return_code |= 1
        elif not self.connected_locations[0].isdir():
            self.log("Source path {rp} is not a directory".format(
                rp=self.connected_locations[0].get_safepath()), self.log.ERROR)
            return_code |= 1

        # check that destination is a directory or doesn't exist
        if (self.connected_locations[1].lstat()
            and not self.connected_locations[1].isdir()):
            if self.values.force:
                self.log("Destination {rp} exists but isn't a directory, "
                         "and will be force deleted".format(
                            rp=self.connected_locations[1].get_safepath()),
                         self.log.WARNING)
            else:
                self.log("Destination {rp} exists and is not a directory, "
                         "call with '--force' to overwrite".format(
                            rp=self.connected_locations[1].get_safepath()),
                         self.log.ERROR)
                return_code |= 1
        # if the target is a non-empty existing directory
        # without rdiff-backup-data sub-directory
        elif (self.connected_locations[1].lstat()
              and self.connected_locations[1].isdir()
              and self.connected_locations[1].listdir()):
            rp_data_dir = self.connected_locations[1].append_path(
                b"rdiff-backup-data")
            if rp_data_dir.lstat():
                previous_time = self._get_mirror_time(rp_data_dir)
                if previous_time >= Time.curtime:
                    self.log("Time of Last backup is not in the past. "
                             "This is probably caused by running two backups "
                             "in less than a second. "
                             "Wait a second and try again.",
                             self.log.ERROR)
                    return_code |= 1
            else:
                if self.values.force:
                    self.log("Target {rp} does not look like a rdiff-backup "
                             "repository but will be force overwritten".format(
                                rp=self.connected_locations[1].get_safepath()),
                             self.log.WARNING)
                else:
                    self.log("Target {rp} does not look like a rdiff-backup "
                             "repository, "
                             "call with '--force' to overwrite".format(
                                rp=self.connected_locations[1].get_safepath()),
                             self.log.ERROR)
                    return_code |= 1

        return return_code

    def setup(self):
        # in setup we return as soon as we detect an issue to avoid changing
        # too much
        return_code = super().setup()
        if return_code != 0:
            return return_code

        # only to type less, consider those variables as immutable
        # (but _not_ the objects they contain)
        self.rp_in = self.connected_locations[0]
        self.rp_out = self.connected_locations[1]

        # make sure the target directory is present
        try:
            # if the target exists and isn't a directory, force delete it
            if (self.rp_out.lstat() and not self.rp_out.isdir()
                and self.values.force):
                self.rp_out.delete()

            # if the target doesn't exist, create it
            if not self.rp_out.lstat():
                if self.values.create_full_path:
                    self.rp_out.makedirs()
                else:
                    self.rp_out.mkdir()
                self.rp_out.chmod(0o700)  # only read-writable by its owner
        except os.error:
            self.log("Unable to delete and/or create directory {rp}".format(
                self.rp_out.get_safepath(), self.log.ERROR)
            return 1

        # define a few essential subdirectories
        self.rp_data_dir = self.rp_out.append_path(b"rdiff-backup-data")
        Globals.rbdir = self.rp_data_dir  # compat200
        self.rp_incs_dir = self.rp_data_dir.append_path(b"increments")
        if not self.rp_data_dir.lstat():
            try
                self.rp_data_dir.mkdir()
            except (OSError, IOError) as exc:
                self.log("Could not create 'rdiff-backup-data' sub-directory "
                         "in '{rp}' due to '{exc}'. "
                         "Please fix the access rights and retry.".format(
                            rp=self.rp_out, exc=exc))
                return 1
        elif self._is_failed_initial_backup(self.rp_data_dir):
            self._fix_failed_initial_backup(self.rp_data_dir)
        if not self.rp_incs_dir.lstat():
            try
                self.rp_incs_dir.mkdir()
            except (OSError, IOError) as exc:
                self.log("Could not create 'increments' sub-directory "
                         "in '{rp}' due to '{exc}'. "
                         "Please fix the access rights and retry.".format(
                            rp=self.rp_data_dir, exc=exc))
                return 1

        SetConnections.UpdateGlobal('rbdir', self.rp_data_dir)  # compat200
        SetConnections.BackupInitConnections(self.rp_in.conn, self.rp_out.conn)
        self.rp_out.conn.fs_abilities.backup_set_globals(self.rp_in,
                                                         self.values.force)
        if self.values.chars_to_quote:
            self.rp_out = self._quoted_rpaths(self.rp_out)
        self._init_user_group_mapping(self.rp_out.conn)
        self._final_init(self.rp_out)
        # FIXME checkdest used to happen here
        self._set_select(self.rp_in)
        self._warn_if_infinite_recursion(rpin, rpout)

    def run(self):
        # do regress the target directory if necessary
        _checkdest_if_necessary(self.rp_out)
        previous_time = self._get_mirror_time()
        if previous_time:
            Time.setprevtime(previous_time)
            rpout.conn.Main.backup_touch_curmirror_local(rpin, rpout)
            backup.Mirror_and_increment(rpin, rpout, _incdir)
            rpout.conn.Main.backup_remove_curmirror_local()
        else:
            backup.Mirror(rpin, rpout)
            rpout.conn.Main.backup_touch_curmirror_local(rpin, rpout)
        rpout.conn.Main.backup_close_statistics(time.time())

    def _is_failed_initial_backup(self, rp_dir):
        """
        Returns True if it looks like the given RPath directory contains
        a failed initial backup, else False.
        """
        if rp_dir.lstat():
            rbdir_files = rp_dir.listdir()
            mirror_markers = [
                x for x in rbdir_files if x.startswith(b"current_mirror")
            ]
            error_logs = [x for x in rbdir_files if x.startswith(b"error_log")]
            metadata_mirrors = [
                x for x in rbdir_files if x.startswith(b"mirror_metadata")
            ]
            # If we have no current_mirror marker, and the increments directory
            # is empty, we most likely have a failed backup.
            return not mirror_markers and len(error_logs) <= 1 and \
                len(metadata_mirrors) <= 1
        return False

    def _fix_failed_initial_backup(self, rp_dir):
        """
        Clear the given rdiff-backup-data if possible, it's faster than
        trying to do a regression, which would probably anyway fail.
        """
        self.log("Found interrupted initial backup in {rp}. "
                 "Removing...".format(rp=rp_dir.get_safepath()),
                 self.log.DEFAULT)
        rbdir_files = rp_dir.listdir()

        # Try to delete the increments dir first
        if b'increments' in rbdir_files:
            rbdir_files.remove(b'increments')
            rp = rp_dir.append(b'increments')
            # FIXME I don't really understand the logic here: either it's
            # a failed initial backup and we can remove everything, or we
            # should fail and not continue.
            try:
                rp.conn.rpath.delete_dir_no_files(rp)
            except rpath.RPathException:
                self.log("Increments dir contains files.", self.log.INFO)
                return
            except Security.Violation:
                self.log("Server doesn't support resuming.", self.log.WARNING)
                return

        # then delete all remaining files
        for file_name in rbdir_files:
            rp = rp_dir.append_path(file_name)
            if not rp.isdir():  # Only remove files, not folders
                rp.delete()


def get_action_class():
    return BackupAction
