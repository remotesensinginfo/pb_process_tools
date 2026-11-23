import datetime
import importlib
import json
import logging
import os
import pathlib
import tempfile
import unittest
import unittest.mock


class PBPTUtilsTest(unittest.TestCase):
    def setUp(self):
        import pbprocesstools.pbpt_utils

        self.pbpt_utils = pbprocesstools.pbpt_utils.PBPTUtils()
        self.text_utils = pbprocesstools.pbpt_utils.PBPTTextFileUtils()
        self.json_helper = pbprocesstools.pbpt_utils.PBPTJSONParseHelper()

    def test_package_init_logging_and_configure(self):
        import pbprocesstools

        self.assertEqual(pbprocesstools.__version__, "4.0.2")
        self.assertIsInstance(pbprocesstools.py_sys_version_flt, float)

        orig_lvl = os.environ.get("PBPT_LOG_LVL")
        orig_cfg = os.environ.get("PBPT_LOG_CFG")
        try:
            os.environ["PBPT_LOG_LVL"] = "INVALID_LEVEL"
            with self.assertRaises(ValueError):
                importlib.reload(pbprocesstools)

            os.environ["PBPT_LOG_LVL"] = "DEBUG"
            with tempfile.TemporaryDirectory() as tmpdir:
                src_cfg = os.path.abspath(
                    os.path.join(
                        os.path.dirname(__file__), "..", "share", "loggingconfig.json"
                    )
                )
                cfg_data = json.loads(pathlib.Path(src_cfg).read_text())
                for handler_cfg in cfg_data.get("handlers", {}).values():
                    if "filename" in handler_cfg:
                        handler_cfg["filename"] = os.path.join(
                            tmpdir, os.path.basename(handler_cfg["filename"])
                        )
                tmp_cfg = os.path.join(tmpdir, "loggingconfig.json")
                pathlib.Path(tmp_cfg).write_text(json.dumps(cfg_data))
                os.environ["PBPT_LOG_CFG"] = tmp_cfg
                importlib.reload(pbprocesstools)
                self.assertEqual(pbprocesstools.log_default_level, logging.DEBUG)
                for h in list(logging.getLogger().handlers):
                    h.close()
                    logging.getLogger().removeHandler(h)
        finally:
            if orig_lvl is None:
                os.environ.pop("PBPT_LOG_LVL", None)
            else:
                os.environ["PBPT_LOG_LVL"] = orig_lvl
            if orig_cfg is None:
                os.environ.pop("PBPT_LOG_CFG", None)
            else:
                os.environ["PBPT_LOG_CFG"] = orig_cfg
            importlib.reload(pbprocesstools)

        mock_root = unittest.mock.MagicMock()
        mock_root.handlers = []
        with unittest.mock.patch("logging.getLogger", return_value=mock_root):
            with unittest.mock.patch("logging.basicConfig") as mock_basic:
                pbprocesstools.configure_logging()
                mock_basic.assert_called_once()

    def test_remove_repeated_chars(self):
        in_str = "aa_bb_cc"
        out_str = self.pbpt_utils.remove_repeated_chars(in_str, "b")
        self.assertEqual(out_str, "aa_b_cc")

        in_str = "aa_bb__cc"
        out_str = self.pbpt_utils.remove_repeated_chars(in_str, "_")
        self.assertEqual(out_str, "aa_bb_cc")

        with self.assertRaises(Exception):
            self.pbpt_utils.remove_repeated_chars("abc", "ab")

    def test_uid_generator(self):
        for size in (4, 6, 8, 12):
            out_str = self.pbpt_utils.uidGenerator(size)
            self.assertEqual(len(out_str), size)

    def test_check_str(self):
        self.assertEqual(
            self.pbpt_utils.check_str("aa_bb-cc", rm_dashs=True), "aa_bb_cc"
        )
        self.assertEqual(
            self.pbpt_utils.check_str("aa!_bb?_cc.", rm_punc=True), "aa_bb_cc"
        )
        self.assertEqual(
            self.pbpt_utils.check_str("aa bb cc", rm_spaces=True), "aa_bb_cc"
        )
        self.assertEqual(
            self.pbpt_utils.check_str("\xc4\xc4_bb_cc", rm_non_ascii=True), "_bb_cc"
        )
        self.assertEqual(
            self.pbpt_utils.check_str("aa! bb? cc.", rm_punc=True, rm_spaces=True),
            "aa_bb_cc",
        )
        self.assertEqual(
            self.pbpt_utils.check_str(
                "\xc4\xc4! bb? cc.", rm_non_ascii=True, rm_spaces=True, rm_punc=True
            ),
            "_bb_cc",
        )
        self.assertEqual(
            self.pbpt_utils.check_str(
                "\xc4\xc4! bb?-cc.",
                rm_non_ascii=True,
                rm_dashs=True,
                rm_spaces=True,
                rm_punc=True,
            ),
            "_bb_cc",
        )
        # Ensure digits are preserved when rm_non_ascii=True
        self.assertEqual(
            self.pbpt_utils.check_str(
                "job_2026-run1",
                rm_non_ascii=True,
                rm_dashs=True,
                rm_spaces=True,
                rm_punc=True,
            ),
            "job_2026_run1",
        )

    def test_text_and_json_file_io(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(FileNotFoundError):
                self.pbpt_utils.readTextFileNoNewLines("/nonexistent/path/db_conn.txt")

            multi_file = os.path.join(tmpdir, "multi.txt")
            pathlib.Path(multi_file).write_text("  postgresql://  \n\n  host/db \n")
            self.assertEqual(
                self.pbpt_utils.readTextFileNoNewLines(multi_file),
                "postgresql://host/db",
            )

            lst_file = os.path.join(tmpdir, "list.txt")
            self.pbpt_utils.writeList2File(["a", "", "b"], lst_file)
            self.assertEqual(self.pbpt_utils.readTextFile2List(lst_file), ["a", "b"])

            data_file = os.path.join(tmpdir, "data.txt")
            self.pbpt_utils.writeData2File(12345, data_file)
            self.assertEqual(pathlib.Path(data_file).read_text(), "12345\n")

            json_file = os.path.join(tmpdir, "data.json")
            payload = {"k": [1, 2], "nested": {"ok": True}}
            self.pbpt_utils.writeDict2JSON(payload, json_file)
            self.assertEqual(self.pbpt_utils.readJSON2Dict(json_file), payload)

            txt_utils_file = os.path.join(tmpdir, "txt_utils.txt")
            self.text_utils.writeList2File(["line1", "", "line2"], txt_utils_file)
            self.assertEqual(
                self.text_utils.readTextFile2List(txt_utils_file), ["line1", "line2"]
            )

    def test_atomic_file_lock_and_clean(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = os.path.join(tmpdir, "test.db")
            self.assertTrue(
                self.pbpt_utils.get_file_lock(
                    target, sleep_period=0.01, wait_iters=1
                )
            )
            # Second acquire while locked fails and raises when use_except=True
            self.assertFalse(
                self.pbpt_utils.get_file_lock(
                    target, sleep_period=0.01, wait_iters=1
                )
            )
            with self.assertRaises(Exception):
                self.pbpt_utils.get_file_lock(
                    target, sleep_period=0.01, wait_iters=0, use_except=True
                )

            # Simulate OSError and RSGISPyException during release triggering clean_file_locks
            import rsgispy

            with unittest.mock.patch(
                "rsgispy.tools.files.release_file_lock",
                side_effect=OSError("perm"),
            ):
                self.pbpt_utils.release_file_lock(target, timeout=3600)
            with unittest.mock.patch(
                "rsgispy.tools.files.release_file_lock",
                side_effect=rsgispy.RSGISPyException("missing"),
            ):
                self.pbpt_utils.release_file_lock(target, timeout=3600)

            self.pbpt_utils.release_file_lock(target)
            self.assertTrue(
                self.pbpt_utils.get_file_lock(
                    target, sleep_period=0.01, wait_iters=1
                )
            )
            self.pbpt_utils.release_file_lock(target)

            # Pre-existing stale lock is automatically cleaned and retried by get_file_lock
            stale_target_lock = os.path.join(tmpdir, ".test.db.lok")
            old_ts = (
                datetime.datetime.now() - datetime.timedelta(seconds=7200)
            ).isoformat()
            pathlib.Path(stale_target_lock).write_text(old_ts)
            self.assertTrue(
                self.pbpt_utils.get_file_lock(
                    target, sleep_period=0.01, wait_iters=0, timeout=10
                )
            )
            self.pbpt_utils.release_file_lock(target)

            # Test clean_file_locks with stale and fresh locks
            stale_lock = os.path.join(tmpdir, ".stale.lok")
            pathlib.Path(stale_lock).write_text(old_ts)

            fresh_lock = os.path.join(tmpdir, ".fresh.lok")
            pathlib.Path(fresh_lock).write_text(datetime.datetime.now().isoformat())

            self.pbpt_utils.clean_file_locks(tmpdir, timeout=3600)
            self.assertFalse(os.path.exists(stale_lock))
            self.assertTrue(os.path.exists(fresh_lock))

            # Clean with empty dir_path falls back to "."
            with unittest.mock.patch(
                "rsgispy.tools.files.clean_file_locks"
            ) as mock_clean:
                self.pbpt_utils.clean_file_locks("", timeout=10)
                mock_clean.assert_called_once_with(".", timeout=10)

    def test_json_helper_full(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            gz_path = os.path.join(tmpdir, "sample.json.gz")
            data = {
                "meta": {
                    "name": "sentinel2",
                    "flag": True,
                    "date": "2026-05-01",
                    "dt": "2026-05-01 12:30:00",
                    "tags": ["a", "b"],
                    "nums": [1, 2, 3],
                },
                "neg": "-5",
                "flt": "3.14",
                "sci": "1e-4",
                "int_val": 42,
                "bool_val": True,
                "bad": "½",
                "bad_tags_type": "not_a_list",
                "bad_tags_item": ["a", 123],
            }
            self.json_helper.writeGZIPJSON(data, gz_path)
            loaded = self.json_helper.readGZIPJSON(gz_path)
            self.assertEqual(loaded["meta"]["name"], "sentinel2")

            self.assertTrue(self.json_helper.doesPathExist(data, ["meta", "name"]))
            self.assertFalse(self.json_helper.doesPathExist(data, ["meta", "missing"]))

            self.assertEqual(
                self.json_helper.getValueDict(data, ["meta"])["name"], "sentinel2"
            )
            with self.assertRaises(KeyError):
                self.json_helper.getValueDict(data, ["meta", "missing"])
            self.assertEqual(
                self.json_helper.getStrValue(
                    data, ["meta", "name"], valid_values=["sentinel2", "landsat8"]
                ),
                "sentinel2",
            )
            with self.assertRaises(Exception):
                self.json_helper.getStrValue(
                    data, ["meta", "name"], valid_values=["landsat8"]
                )
            with self.assertRaises(Exception):
                self.json_helper.getStrValue(data, ["meta", "nonexistent"])

            self.assertTrue(self.json_helper.getBooleanValue(data, ["meta", "flag"]))
            with self.assertRaises(Exception):
                self.json_helper.getBooleanValue(data, ["meta", "name"])

            self.assertEqual(
                self.json_helper.getDateValue(data, ["meta", "date"]),
                datetime.date(2026, 5, 1),
            )
            self.assertEqual(
                self.json_helper.getDateTimeValue(
                    data, ["meta", "dt"], "%Y-%m-%d %H:%M:%S"
                ),
                datetime.datetime(2026, 5, 1, 12, 30, 0),
            )

            self.assertEqual(
                self.json_helper.getStrListValue(
                    data, ["meta", "tags"], valid_values=["a", "b", "c"]
                ),
                ["a", "b"],
            )
            with self.assertRaises(Exception):
                self.json_helper.getStrListValue(data, ["bad_tags_type"])
            with self.assertRaises(Exception):
                self.json_helper.getStrListValue(
                    data, ["bad_tags_item"], valid_values=["a"]
                )
            with self.assertRaises(Exception):
                self.json_helper.getStrListValue(
                    data, ["meta", "tags"], valid_values=["only_x"]
                )

            self.assertEqual(self.json_helper.getListValue(data, ["meta", "nums"]), [1, 2, 3])
            with self.assertRaises(Exception):
                self.json_helper.getListValue(data, ["bad_tags_type"])

            self.assertEqual(self.json_helper.getNumericValue(data, ["neg"]), -5.0)
            self.assertAlmostEqual(
                self.json_helper.getNumericValue(data, ["flt"]), 3.14
            )
            self.assertAlmostEqual(
                self.json_helper.getNumericValue(data, ["sci"]), 1e-4
            )
            self.assertEqual(
                self.json_helper.getNumericValue(
                    data, ["int_val"], valid_lower=10, valid_upper=50
                ),
                42,
            )
            with self.assertRaises(Exception):
                self.json_helper.getNumericValue(data, ["int_val"], valid_lower=100)
            with self.assertRaises(Exception):
                self.json_helper.getNumericValue(data, ["int_val"], valid_upper=10)
            with self.assertRaises(Exception):
                self.json_helper.getNumericValue(data, ["bool_val"])
            with self.assertRaises(Exception):
                self.json_helper.getNumericValue(data, ["bad"])


if __name__ == "__main__":
    unittest.main()

