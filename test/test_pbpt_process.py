import os
import pathlib
import re
import sys
import tempfile
import types
import unittest
import unittest.mock

from pbprocesstools.pbpt_process import PBPTProcessToolsBase


class PBPTProcessToolsBaseTest(unittest.TestCase):
    def setUp(self):
        self.tool = PBPTProcessToolsBase(uid_len=6)

    def test_timestamp_and_basename(self):
        ts = self.tool.generate_readable_timestamp_str()
        self.assertRegex(ts, r"^\d{8}_\d{6}$")

        self.assertEqual(
            self.tool.get_file_basename("/tmp/dir/scene_2026_v1.tif"),
            "scene_2026_v1",
        )
        self.assertEqual(
            self.tool.get_file_basename(
                "/tmp/my file!_name?.kea", check_valid=True
            ),
            "my_file_name",
        )
        self.assertEqual(
            self.tool.get_file_basename(
                "/tmp/a_b_c_d.tif", n_comps=2
            ),
            "a_b",
        )
        with self.assertRaises(Exception):
            self.tool.get_file_basename("/tmp/only_two.tif", n_comps=5)

    def test_create_dir_and_remove_dir(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "work_dir")
            created_path, created = self.tool.create_dir(target, add_uid=True)
            self.assertTrue(created)
            self.assertTrue(os.path.isdir(created_path))
            self.assertEqual(
                os.path.dirname(created_path), str(pathlib.Path(tmpdir).resolve())
            )
            self.assertTrue(os.path.basename(created_path).startswith("work_dir_"))

            # Re-creating existing directory returns created=False
            plain_dir = os.path.join(tmpdir, "plain")
            _, c1 = self.tool.create_dir(plain_dir, use_abs_path=False, add_uid=False)
            _, c2 = self.tool.create_dir(plain_dir, use_abs_path=False, add_uid=False)
            self.assertTrue(c1)
            self.assertFalse(c2)

            pathlib.Path(os.path.join(plain_dir, "f.txt")).write_text("x")
            self.tool.remove_dir(plain_dir)
            self.assertFalse(os.path.exists(plain_dir))
            # Removing already-absent directory is safe no-op
            self.tool.remove_dir(plain_dir)

    def test_has_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertFalse(self.tool.has_files(tmpdir))
            self.assertFalse(PBPTProcessToolsBase.has_files(tmpdir))

            subdir = os.path.join(tmpdir, "sub")
            os.makedirs(subdir)
            subfile = os.path.join(subdir, "data.txt")
            pathlib.Path(subfile).write_text("hello")

            self.assertFalse(self.tool.has_files(tmpdir, recursive=False))
            self.assertTrue(self.tool.has_files(tmpdir, recursive=True))

            with self.assertRaises(NotADirectoryError):
                PBPTProcessToolsBase.has_files(subfile)

    def test_find_file_and_find_first_file(self):
        self.assertIsNone(self.tool.find_file([], "*.txt"))
        self.assertIsNone(
            self.tool.find_first_file("/nonexistent/dir/path", "*.txt", rtn_except=False)
        )
        with self.assertRaises(Exception):
            self.tool.find_first_file(
                "/nonexistent/dir/path", "*.txt", rtn_except=True
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            d1 = os.path.join(tmpdir, "d1")
            d2 = os.path.join(tmpdir, "d2")
            nested = os.path.join(d2, "nested")
            os.makedirs(d1)
            os.makedirs(nested)

            f1 = os.path.join(nested, "one.tif")
            pathlib.Path(f1).write_text("a")

            self.assertIsNone(self.tool.find_file(d1, "*.tif"))
            self.assertEqual(self.tool.find_file(nested, "*.tif"), f1)
            self.assertEqual(self.tool.find_file([d1, nested], "*.tif"), f1)
            self.assertEqual(self.tool.find_first_file(tmpdir, "*.tif"), f1)

            f2 = os.path.join(nested, "two.tif")
            pathlib.Path(f2).write_text("b")
            # Multiple matches returns None for find_file and raises/None for find_first_file
            self.assertIsNone(self.tool.find_file(nested, "*.tif"))
            self.assertIsNone(
                self.tool.find_first_file(nested, "*.tif", rtn_except=False)
            )
            with self.assertRaises(Exception):
                self.tool.find_first_file(nested, "*.tif", rtn_except=True)

    def test_check_files_variants_and_rsgislib_checks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ok, errs = self.tool.check_files({tmpdir: "filesize"})
            self.assertFalse(ok)
            self.assertIn(tmpdir, errs)

            empty_file = os.path.join(tmpdir, "empty.txt")
            pathlib.Path(empty_file).touch()
            ok, errs = self.tool.check_files({empty_file: "filesize"})
            self.assertFalse(ok)
            self.assertIn(empty_file, errs)

            nonempty_file = os.path.join(tmpdir, "full.txt")
            pathlib.Path(nonempty_file).write_text("ok")
            ok, errs = self.tool.check_files({nonempty_file: "filesize"})
            self.assertTrue(ok)
            self.assertEqual(errs, {})

            # Missing file & plain "file" check & invalid dict without "type"
            missing = os.path.join(tmpdir, "missing.txt")
            ok, errs = self.tool.check_files({missing: "file", nonempty_file: "file"})
            self.assertFalse(ok)
            self.assertIn(missing, errs)

            with self.assertRaises(Exception):
                self.tool.check_files({nonempty_file: {"n_bands": 3}})

            # Without rsgislib available, gdal_image / gdal_vector / hdf5 raise Exception
            no_rsgis = {
                "rsgislib": None,
                "rsgislib.tools": None,
                "rsgislib.tools.checkdatasets": None,
            }
            with unittest.mock.patch.dict(sys.modules, no_rsgis):
                for fmt in ("gdal_image", "gdal_vector", "hdf5"):
                    with self.assertRaises(Exception):
                        self.tool.check_files({nonempty_file: fmt})

            # With mocked rsgislib.tools.checkdatasets, test both pass and fail for string and dict specs
            fake_rsgis = types.ModuleType("rsgislib")
            fake_tools = types.ModuleType("rsgislib.tools")
            fake_chk = types.ModuleType("rsgislib.tools.checkdatasets")
            fake_chk.run_check_gdal_image_file = unittest.mock.MagicMock(return_value=True)
            fake_chk.run_check_gdal_vector_file = unittest.mock.MagicMock(return_value=True)
            fake_chk.run_check_hdf5_file = unittest.mock.MagicMock(return_value=True)
            fake_tools.checkdatasets = fake_chk
            fake_rsgis.tools = fake_tools

            with unittest.mock.patch.dict(
                sys.modules,
                {
                    "rsgislib": fake_rsgis,
                    "rsgislib.tools": fake_tools,
                    "rsgislib.tools.checkdatasets": fake_chk,
                },
            ):
                ok, errs = self.tool.check_files(
                    {
                        nonempty_file: "gdal_image",
                        empty_file: {
                            "type": "gdal_image",
                            "n_bands": 3,
                            "chk_proj": True,
                            "epsg_code": 4326,
                            "read_img": True,
                        },
                    }
                )
                self.assertTrue(ok)
                self.assertEqual(errs, {})

                ok, errs = self.tool.check_files(
                    {
                        nonempty_file: "gdal_vector",
                        empty_file: {
                            "type": "gdal_vector",
                            "chk_proj": True,
                            "epsg_code": 27700,
                        },
                    }
                )
                self.assertTrue(ok)

                ok, errs = self.tool.check_files(
                    {
                        nonempty_file: "hdf5",
                        empty_file: {"type": "hdf5", "timeout": 2},
                    }
                )
                self.assertTrue(ok)

                # Simulate failing checks
                fake_chk.run_check_gdal_image_file.return_value = False
                fake_chk.run_check_gdal_vector_file.return_value = False
                fake_chk.run_check_hdf5_file.return_value = False
                for fmt in ("gdal_image", "gdal_vector", "hdf5"):
                    ok, errs = self.tool.check_files({nonempty_file: fmt})
                    self.assertFalse(ok)
                    self.assertIn(nonempty_file, errs)


if __name__ == "__main__":
    unittest.main()

