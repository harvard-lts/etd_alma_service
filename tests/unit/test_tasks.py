import tasks.tasks as tasks

FEATURE_FLAGS = "feature_flags"


class TestTasksClass():

    def test_send_to_alma(self):
        message = {"unit_test": "true", FEATURE_FLAGS: {
                'dash_feature_flag': "on",
                'alma_feature_flag': "on",
                'send_to_drs_feature_flag': "off",
                'drs_holding_record_feature_flag': "off"},
                "identifier": "30522803"}
        retval = tasks.send_to_alma(message)
        assert "hello" in retval
        assert "feature_flags" in retval
        assert "identifier" in retval
        assert retval["identifier"] == "30522803"
