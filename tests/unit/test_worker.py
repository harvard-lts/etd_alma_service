from etd.worker import Worker
from etd.worker import getFromMets
from etd.worker import writeMarcXml
import requests
import lxml.etree as ET
import shutil
import os

generatedMarcXmlValues = None
abstractText = 'The "Naming Expeditor" project aims to demystify the institutional process and principles of naming, including denaming and renaming, at Harvard University. Through research on historical archives and contemporary testimonies, the project seeks to understand the meaning-constructive nature of naming as a dynamic and iterative device in the public realm. By integrating theories and practices from the realms of art and activism, the project explores alternative channels, forms, and tools to reimagine a collective naming process and system that amplifies community voices with increased awareness, democracy, and participation. The project\'s final deliverable contains an image essay analyzing the current institutional naming system at Harvard and proposing the niche and strategies of an agency named "Naming Expeditor", as well as a live performance, aiming to display the power behind the texts in the existing institutional naming system, and evoke public discussion and actions in the broader community.'

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

    def test_send_to_alma(self, monkeypatch): #tbd
        assert True
    
    def test_getFromMets(self, monkeypatch):
        metsFile = "/home/etdadm/tests/data/in/proquest2023071720-993578-gsd/mets_before.xml"
        verbose = False
        marcXmlValues = getFromMets(metsFile, verbose)
        global generatedMarcXmlValues 
        generatedMarcXmlValues = marcXmlValues
        assert marcXmlValues['proquestId'] == '30522803'
        assert marcXmlValues['author'] == 'Peng, Yolanda Yuanlu'
        assert marcXmlValues['title'] == 'Naming Expeditor: Reimagining ' \
            'Institutional Naming System at Harvard'
        assert marcXmlValues['dateCreated'] == '2023'
        assert marcXmlValues['degreeName'] == 'MDes'
        assert marcXmlValues['degreeLevel'] == "Master's"
        assert marcXmlValues['degreeTranslation'] == 'Master in Design Studies'
        assert marcXmlValues['school'] == 'Harvard Graduate School of Design'
        assert marcXmlValues['department'] == 'Department of Urban Planning and Design'
        advisors = marcXmlValues['advisors']
        assert advisors[0] == 'Shoshan, Malkit'
        committeeMembers = marcXmlValues['committeeMembers']
        assert committeeMembers[0] == 'Bruguera, Tania'
        assert committeeMembers[1] == 'Dhillon, Nitasha'
        assert committeeMembers[2] == 'Wodiczko, Krzysztof'
        assert committeeMembers[3] == 'Naginski, Erika'
        assert committeeMembers[4] == 'Claudio, Yazmin C'
        assert marcXmlValues['description'] == abstractText

    def test_writeMarcXml(self, monkeypatch):
        batch = "alma2023071720-993578-gsd"
        batchOutputDir = "/home/etdadm/tests/data/in/proquest2023071720-993578-gsd"
        verbose = False
        global generatedMarcXmlValues
        writeMarcXml(batch, batchOutputDir, generatedMarcXmlValues, verbose)
        metsFile = batchOutputDir + "/" + batch + ".xml"
        assert os.path.exists(metsFile)
        doc = ET.parse(metsFile)
        assert doc.xpath("//record/datafield[@tag='100']/subfield[@code='a']")[0].text == "Peng, Yolanda Yuanlu"
        assert doc.xpath("//record/datafield[@tag='100']/subfield[@code='c']")[0].text == "(MDes, Harvard University, 2023)"
        assert doc.xpath("//record/datafield[@tag='245']/subfield[@code='a']")[0].text == "Naming Expeditor: Reimagining Institutional Naming System at Harvard"
        assert doc.xpath("//record/datafield[@tag='502']/subfield[@code='a']")[0].text == "Thesis (MDes, Master in Design Studies, Department of Urban Planning and Design)--Harvard Graduate School of Design, May 2023."
        assert doc.xpath("//record/datafield[@tag='720']/subfield[@code='a']")[0].text == "Shoshan, Malkit"
        assert doc.xpath("//record/datafield[@tag='720']/subfield[@code='a']")[1].text == "Bruguera, Tania,"
        assert doc.xpath("//record/datafield[@tag='720']/subfield[@code='a']")[5].text == "Claudio, Yazmin C,"
        assert doc.xpath("//record/datafield[@tag='520']/subfield[@code='a']")[0].text == abstractText
