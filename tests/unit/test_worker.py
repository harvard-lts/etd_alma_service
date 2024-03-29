from etd.worker import Worker
from etd.worker import getFromMets
from etd.worker import writeMarcXml
from etd.worker import escapeStr
from etd.worker import cleanMetsFile
from etd.worker import normalizeLookupKey
from etd.worker import existsInDash
import requests
import lxml.etree as ET
import os
import shutil
from etd.etds2alma_tables import degreeLevelTracing

generatedMarcXmlValues = None
abstractText = 'The "Naming Expeditor" project aims to demystify the ' \
               'institutional process and principles of naming, including ' \
               'denaming and renaming, at Harvard University. ' \
               'Through research on historical archives and contemporary ' \
               'testimonies, the project seeks to understand the ' \
               'meaning-constructive nature of naming as a dynamic and ' \
               'iterative device in the public realm. By integrating ' \
               'theories and practices from the realms of art and activism, ' \
               'the project explores alternative channels, forms, and tools ' \
               'to reimagine a collective naming process and system that ' \
               'amplifies community voices with increased awareness, ' \
               'democracy, and participation. The project\'s final ' \
               'deliverable contains an image essay analyzing the current ' \
               'institutional naming system at Harvard and proposing the ' \
               'niche and strategies of an agency named "Naming Expeditor", ' \
               'as well as a live performance, aiming to display the power ' \
               'behind the texts in the existing institutional naming ' \
               'system, and evoke public discussion and actions in the '\
               'broader community.'


class MockResponse:
    text = "REST api is running."


class TestWorkerClass():

    def test_version(self):
        expected_version = "0.0.1"
        worker = Worker()
        version = worker.get_version()
        assert version == expected_version

    def test_api(self, monkeypatch):

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is running."
        worker = Worker()
        msg = worker.call_api()
        assert msg == expected_msg

    def test_api_fail(self, monkeypatch):

        def mock_get(*args, **kwargs):
            return MockResponse()

        # apply the monkeypatch for requests.get to mock_get
        monkeypatch.setattr(requests, "get", mock_get)
        expected_msg = "REST api is NOT running."
        worker = Worker()
        msg = worker.call_api()
        assert msg != expected_msg

    def test_send_to_alma(self, monkeypatch):  # tbd
        assert True

    def test_getFromMets(self, monkeypatch):
        metsFile = "./tests/data/in/" \
                   "proquest2023071720-993578-gsd/mets_before.xml"
        verbose = False
        marcXmlValues = getFromMets(metsFile, verbose)
        global generatedMarcXmlValues
        generatedMarcXmlValues = marcXmlValues
        generatedMarcXmlValues['dash_id'] = '993578'
        assert marcXmlValues['proquestId'] == '30522803'
        assert marcXmlValues['author'] == 'Peng, Yolanda Yuanlu'
        assert marcXmlValues['title'] == 'Naming Expeditor: Reimagining ' \
            'Institutional Naming System at Harvard'
        assert marcXmlValues['dateCreated'] == '2023'
        assert marcXmlValues['degreeName'] == 'MDes'
        assert marcXmlValues['degreeLevel'] == "Master's"
        assert marcXmlValues['degreeTranslation'] == 'Master in Design Studies'
        assert marcXmlValues['school'] == 'Harvard Graduate School of Design'
        assert marcXmlValues['department'] == 'Department of ' \
                                              'Urban Planning and Design'
        advisors = marcXmlValues['advisors']
        assert advisors[0] == 'Shoshan, Malkit'
        committeeMembers = marcXmlValues['committeeMembers']
        assert committeeMembers[0] == 'Bruguera, Tania'
        assert committeeMembers[1] == 'Dhillon, Nitasha'
        assert committeeMembers[2] == 'Wodiczko, Krzysztof'
        assert committeeMembers[3] == 'Naginski, Erika'
        assert committeeMembers[4] == 'Claudio, Yazmin C'
        assert marcXmlValues['description'] == abstractText

    def test_getFromMets_incorrect_degree(self, monkeypatch):
        metsFile = "./tests/data/in/" \
                   "proquest2023071720-993578-gsd-incorrect-degree/" \
                   "mets_with_incorrect_degree_code.xml"
        verbose = False
        marcXmlValues = getFromMets(metsFile, verbose)
        assert not marcXmlValues

    def test_writeMarcXml(self, monkeypatch):
        batch = "alma2023071720-993578-gsd"
        batchOutputDir = "./tests/data/in/proquest2023071720-993578-gsd"
        verbose = False
        global generatedMarcXmlValues
        writeMarcXml(batch, batchOutputDir, generatedMarcXmlValues, verbose)
        marcFile = batchOutputDir + "/" + batch + ".xml"
        assert os.path.exists(marcFile)
        namespace_mapping = {'marc': 'http://www.loc.gov/MARC21/slim'}
        doc = ET.parse(marcFile)
        authorXPath = "//marc:record/marc:datafield[@tag='100']" \
                      "/marc:subfield[@code='a']"
        degreeXPath = "//marc:record/marc:datafield[@tag='100']" \
                      "/marc:subfield[@code='c']"
        titleXPath = "//marc:record/marc:datafield[@tag='245']" \
                     "/marc:subfield[@code='a']"
        schoolXPath = "//marc:record/marc:datafield[@tag='502']" \
                      "/marc:subfield[@code='a']"
        advisorXpath = "//marc:record/marc:datafield[@tag='720']" \
                       "/marc:subfield[@code='a']"
        abstractXPath = "//marc:record/marc:datafield[@tag='520']" \
                        "/marc:subfield[@code='a']"
        dashXpath = "//marc:record/marc:datafield[@tag='856']" \
                    "/marc:subfield[@code='u']"
        assert doc.xpath(authorXPath,
                         namespaces=namespace_mapping)[0].text == \
            "Peng, Yolanda Yuanlu"
        assert doc.xpath(degreeXPath,
                         namespaces=namespace_mapping)[0].text == \
            "(MDes, Harvard University, 2023)"
        assert doc.xpath(titleXPath, namespaces=namespace_mapping)[0].text == \
            "Naming Expeditor: " \
            "Reimagining Institutional Naming System at Harvard"
        assert doc.xpath(schoolXPath,
                         namespaces=namespace_mapping)[0].text == \
            "Thesis (MDes, Master in Design Studies, " \
            "Department of Urban Planning and Design) -- " \
            "Harvard Graduate School of Design, May 2023."
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[0].text == \
            "Shoshan, Malkit,"
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[1].text == \
            "Bruguera, Tania,"
        assert doc.xpath(advisorXpath,
                         namespaces=namespace_mapping)[5].text == \
            "Claudio, Yazmin C,"
        assert doc.xpath(abstractXPath,
                         namespaces=namespace_mapping)[0].text == \
            abstractText
        assert doc.xpath(dashXpath,
                         namespaces=namespace_mapping)[0].text == \
            "https://nrs.harvard.edu/urn-3:HUL.InstRepos:993578"

    def test_escapeStr(self):
        line = "     “<This & That  Tests>”   \u0000\u0009\u000a\u000c\u000d"
        newLine = escapeStr(line)
        assert newLine == ' "&lt;This &amp; That Tests&gt;" '

    def test_cleanMetsFile(self):
        testFile = "./tests/data/test.xml"
        resetFile = "./tests/data/test.xml.orig"
        cleanMetsFile(testFile)
        assert os.path.exists(testFile)
        f = open(testFile)
        contents = f.read()
        f.close()
        # reset file
        shutil.copy2(resetFile, testFile)
        assert contents == "<xml?><test>pass</test></xml>"

    def test_problemFile(self):
        problemFile = "./tests/data/problem.xml"
        resetFile = "./tests/data/problem.xml.orig"
        verbose = False
        marcXmlValues = getFromMets(problemFile, verbose)
        shutil.copy2(resetFile, problemFile)
        assert marcXmlValues['proquestId'] == '28769235'
        assert marcXmlValues['author'] == 'Zinn, Eric Michael'
        assert marcXmlValues['title'] == 'Combinatorial Ancestral AAV ' \
            'Capsid Libraries Enable Multidimensional Study of Vector Biology'

    def test_degreeLevelLookup(self):
        assert generatedMarcXmlValues['degreeLevel'] == "Master's"
        assert degreeLevelTracing[generatedMarcXmlValues['degreeLevel']] == \
            "Theses"
        assert degreeLevelTracing[normalizeLookupKey("Doctoral Dissertation")]\
            == "Dissertations"
        assert degreeLevelTracing["Master's"] == "Theses"
        assert degreeLevelTracing["Bachelors"] == "Theses"

    def test_is_in_dash_true(self):
        mapFile = "./tests/data/mapfile"
        result = existsInDash(mapFile)
        # Assert that the function returns True
        assert result

    def test_is_in_dash_false(self):
        emptyMapFile = "./tests/data/mapfile.empty"
        result = existsInDash(emptyMapFile)
        # Assert that the function returns False
        assert not result
