import unittest


class HeftBasedOpeartorsTest(unittest.TestCase):
    def test_force_vector_matrix(self):

        pass
    pass

if __name__ == "main":
    suite = unittest.TestLoader().loadTestsFromTestCase(HeftBasedOpeartorsTest)
    unittest.TextTestRunner(verbosity=2).run(suite)