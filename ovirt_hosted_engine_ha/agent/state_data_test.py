__author__ = 'msivak'
import state_data
import unittest


class TestCases(unittest.TestCase):
    @staticmethod
    def cpu_load(time, load):
        """
        Prepares stats structure with timestamp and cpu-load info
        """
        return state_data.StatsData(
            collect_start=time,
            collect_finish=time,
            metadata_too_new=False,
            cluster={},
            host_id=1,
            hosts={},
            local={
                "cpu-load": load
            },
            maintenance=False
        )

    @staticmethod
    def he_historic_data(history):
        """
        Prepares dummy HostedEngineData with provided history
        """
        sorted_history = history[:]
        sorted_history.sort(reverse=True, key=lambda s: s.collect_start)
        s = {"history": sorted_history,
             "stats": sorted_history[0],
             "host_id": sorted_history[0].host_id}
        for k in state_data.HostedEngineData._fields:
            s.setdefault(k, None)
        return state_data.HostedEngineData(**s)

    def test_cpu_load_zero(self):
        history = []
        history.append(self.cpu_load(0, 0))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 0.0)

    def test_cpu_load_const(self):
        history = []
        history.append(self.cpu_load(0, 5))
        history.append(self.cpu_load(1, 5))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 5.0)

    def test_cpu_load_grow(self):
        history = []
        history.append(self.cpu_load(0, 0))
        history.append(self.cpu_load(1, 10))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 5.0)

    def test_cpu_load_grow_fall(self):
        history = []
        history.append(self.cpu_load(0, 0))
        history.append(self.cpu_load(1, 10))
        history.append(self.cpu_load(2, 0))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 5.0)

    def test_cpu_load_grow_fall_nonlinear(self):
        history = []
        history.append(self.cpu_load(0, 0))
        history.append(self.cpu_load(1, 30))
        history.append(self.cpu_load(3, 0))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 15.0)

    def test_const_nonlinear(self):
        history = []
        history.append(self.cpu_load(0, 5))
        history.append(self.cpu_load(1, 5))
        history.append(self.cpu_load(3, 5))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 5.0)

    def test_const_nonlinear_with_hole(self):
        history = []
        history.append(self.cpu_load(0, 5))
        history.append(self.cpu_load(1, 5))
        history.append(self.cpu_load(2, None))
        history.append(self.cpu_load(5, 5))
        he_data = self.he_historic_data(history)
        self.assertEqual(state_data.load_factor(he_data), 5.0)
