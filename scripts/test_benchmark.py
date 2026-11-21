import unittest

from benchmark import (parse_job_report, parse_producer_report, parse_size, parse_stats_line,
                       percentile)


class ParseSizeTest(unittest.TestCase):
    def test_binary_and_decimal_units(self):
        self.assertEqual(parse_size("512B"), 512)
        self.assertEqual(parse_size("1.5GiB"), 1.5 * 2**30)
        self.assertEqual(parse_size("300kB"), 300e3)
        self.assertEqual(parse_size(" 12.3MiB "), 12.3 * 2**20)

    def test_rejects_unknown_unit(self):
        with self.assertRaises(ValueError):
            parse_size("3 parsecs")


class ParseStatsLineTest(unittest.TestCase):
    LINE = ('{"CPUPerc":"123.45%","MemUsage":"1.2GiB / 7.6GiB","Name":"ssdm-kafka-1",'
            '"MemPerc":"15.79%","ID":"abc"}')

    def test_parses_cpu_and_memory(self):
        self.assertEqual(parse_stats_line(self.LINE), ("ssdm-kafka-1", 123.45, 1.2 * 2**30))

    def test_strips_screen_clearing_codes_of_streaming_mode(self):
        self.assertEqual(parse_stats_line("\x1b[2J\x1b[H" + self.LINE + "\n")[0], "ssdm-kafka-1")

    def test_skips_blank_and_starting_containers(self):
        self.assertIsNone(parse_stats_line("\x1b[2J\x1b[H"))
        self.assertIsNone(parse_stats_line('{"CPUPerc":"--","MemUsage":"-- / --","Name":"x"}'))


class ReportTest(unittest.TestCase):
    def test_job_report(self):
        output = "Job running\nProcessed 10000 events in 2.5 s (4000 events/s) at parallelism 4\n"
        self.assertEqual(parse_job_report(output), (10000, 2.5, 4000))

    def test_producer_report(self):
        output = "Sent 9990 events to latency-ticks in 10.0 s (999 events/s); dropped 10 late events\n"
        self.assertEqual(parse_producer_report(output), (9990, 999))

    def test_missing_report_fails(self):
        with self.assertRaises(RuntimeError):
            parse_job_report("Could not run evaluation")


class PercentileTest(unittest.TestCase):
    def test_nearest_rank(self):
        values = [float(v) for v in range(1, 101)]
        self.assertEqual(percentile(values, 0.50), 50)
        self.assertEqual(percentile(values, 0.99), 99)
        self.assertEqual(percentile(values, 1.0), 100)
        self.assertEqual(percentile([7.0], 0.5), 7)


if __name__ == "__main__":
    unittest.main()
