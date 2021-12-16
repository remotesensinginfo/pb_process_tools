import unittest


class PBPTUtilsTest(unittest.TestCase):

    def setUp(self):
        import pbprocesstools.pbpt_utils
        self.pbpt_utils = pbprocesstools.pbpt_utils.PBPTUtils()

    def test_remove_repeated_chars(self):
        in_str = "aa_bb_cc"
        out_str = self.pbpt_utils.remove_repeated_chars(in_str, 'b')
        self.assertEqual(out_str, "aa_b_cc")

        in_str = "aa_bb__cc"
        out_str = self.pbpt_utils.remove_repeated_chars(in_str, '_')
        self.assertEqual(out_str, "aa_bb_cc")

    def test_uid_generator(self):
        out_str = self.pbpt_utils.uidGenerator(4)
        self.assertEqual(len(out_str), 4)

        out_str = self.pbpt_utils.uidGenerator(6)
        self.assertEqual(len(out_str), 6)

        out_str = self.pbpt_utils.uidGenerator(8)
        self.assertEqual(len(out_str), 8)

        out_str = self.pbpt_utils.uidGenerator(12)
        self.assertEqual(len(out_str), 12)

    def test_check_str(self):
        str_val = "aa_bb-cc"
        out_str = self.pbpt_utils.check_str(str_val, rm_dashs=True)
        self.assertEqual(out_str, "aa_bb_cc")

        str_val = "aa!_bb?_cc."
        out_str = self.pbpt_utils.check_str(str_val, rm_punc=True)
        self.assertEqual(out_str, "aa_bb_cc")

        str_val = "aa bb cc"
        out_str = self.pbpt_utils.check_str(str_val, rm_spaces=True)
        self.assertEqual(out_str, "aa_bb_cc")

        str_val = "\xc4\xc4_bb_cc"
        out_str = self.pbpt_utils.check_str(str_val, rm_non_ascii=True)
        self.assertEqual(out_str, "_bb_cc")

        str_val = "aa! bb? cc."
        out_str = self.pbpt_utils.check_str(str_val, rm_punc=True, rm_spaces=True)
        self.assertEqual(out_str, "aa_bb_cc")

        str_val = "\xc4\xc4! bb? cc."
        out_str = self.pbpt_utils.check_str(str_val, rm_non_ascii=True, rm_spaces=True, rm_punc=True)
        self.assertEqual(out_str, "_bb_cc")

        str_val = "\xc4\xc4! bb?-cc."
        out_str = self.pbpt_utils.check_str(str_val, rm_non_ascii=True, rm_dashs=True, rm_spaces=True, rm_punc=True)
        self.assertEqual(out_str, "_bb_cc")




if __name__ == '__main__':
    unittest.main()
